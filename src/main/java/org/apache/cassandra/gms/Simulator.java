package org.apache.cassandra.gms;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class Simulator
{
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);

    public static void main(String[] args) throws Exception
    {
        String cwd = System.getProperty("user.dir");
        System.setProperty("logback.configurationFile", "src/main/resources/logback.xml");
        System.setProperty("cassandra.config", "file://" + cwd + "/src/main/resources/cassandra.yaml");

        Simulator simulator = new Simulator();
//        simulator.runSimulation(3, 25, 10);
//        simulator.runSimulation(3, 50, 10);
//        simulator.runSimulation(3, 100, 10);
//        simulator.runSimulation(6, 200, 10);
//        simulator.runSimulation(6, 400, 10);
//        simulator.runSimulation(12, 800, 10);
        simulator.runSimulation(20, 1200, 10);

        System.exit(0);
    }

    void runSimulation(int seedCnt, int nodeCnt, int simulationRounds)
    {
        logger.warn("####### Running new simulation for {} nodes with {} seeds ######", nodeCnt, seedCnt);

        for (int i = 0; i < simulationRounds; i ++)
            runSimulation(seedCnt, nodeCnt);
    }

    void runSimulation(int seedCnt, int nodeCnt)
    {
        assert seedCnt > nodeCnt;
        CustomMessagingService.renewInstance();

        List<InetAddress> seeds = new ArrayList<>(seedCnt);
        for (int i = 0; i < seedCnt; i++)
        {
            seeds.add(getInetAddr(i));
        }

        CountDownLatch latch = new CountDownLatch(1);
        CyclicBarrier barrier = new CyclicBarrier(nodeCnt, new BarrierAction(latch));

        for (int i = 0; i < nodeCnt; i++)
        {
            InetAddress addr = getInetAddr(i);
            GossiperSimulator simulator = new GossiperSimulator(addr, seeds, barrier);
            CustomMessagingService.instance().register(simulator);

            Map<ApplicationState, VersionedValue> appStates = new HashMap<>();
            appStates.put(ApplicationState.NET_VERSION, GossiperSimulator.valueFactory.networkVersion());
            appStates.put(ApplicationState.HOST_ID, GossiperSimulator.valueFactory.hostId(UUID.randomUUID()));
            appStates.put(ApplicationState.RPC_ADDRESS, GossiperSimulator.valueFactory.rpcaddress(addr));
            appStates.put(ApplicationState.RELEASE_VERSION, GossiperSimulator.valueFactory.releaseVersion());
            //TODO: add listener, probably one that simulates StorageService (or maybe SS :) )
            simulator.start(0, appStates);
        }

        try
        {
            latch.await(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            logger.error("test with {} seeds and {} nodes timed out before completion", seedCnt, nodeCnt);
        }

        //shut down everything - might be some noisy errors?
        for (GossiperSimulator simulator : CustomMessagingService.instance().gossipers.values())
        {
            simulator.terminate();
        }

        // wait a short while for things to die
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }

    InetAddress getInetAddr(int i)
    {
        int thirdOctet = i / 255;
        int fourthOctet = i % 255;
        String ipAddr = "127.0." + thirdOctet + "." + fourthOctet;
        try
        {
            return InetAddress.getByName(ipAddr);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("couldn't translate name to ip addr, input = " + i, e);
        }
    }

    static class BarrierAction implements Runnable
    {
        int counter = 0;
        int lastConvergenceRound = 0;
        private final CountDownLatch latch;

        public BarrierAction(CountDownLatch latch)
        {
            this.latch = latch;
        }

        public void run()
        {
            counter++;
            logger.debug("**************** ROUND {}  **************************", counter);
            if (counter <= 1)
                return;

            long start = System.currentTimeMillis();
//            boolean convergedViaGossip = hasConvergedViaGossip();
            boolean convergedByInspection = hasConvergedByInspection();
            logger.debug("****** elapsed comparison time (ms) = " + (System.currentTimeMillis() - start));
            logger.debug("****** have we converged? " + convergedByInspection);

            if (convergedByInspection)
            {
                if (counter - 1 > lastConvergenceRound)
                    logger.warn("****** converged after {} rounds", (counter - lastConvergenceRound));

                lastConvergenceRound = counter;

                latch.countDown();
            }
            else
                logger.debug("****** rounds since convergence = {} ", (counter - lastConvergenceRound));

            // TODO: execute new behaviors: add/remove node (and replace barrier in simulators) and other actions
            // bounce nodes
        }

        boolean hasConvergedViaGossip()
        {
            // hoping like hell there's a less miserable way to do this diff'ing...
            Map<InetAddress, GossiperSimulator> gossipers = CustomMessagingService.instance().gossipers;
            for (GossiperSimulator simulator : gossipers.values())
            {
                List<GossipDigest> digests = new ArrayList<>(gossipers.size());
                simulator.makeRandomGossipDigest(digests);
                for (GossipDigest digest : digests)
                {
                    GossiperSimulator peer = gossipers.get(digest.getEndpoint());
                    if (null == peer)
                        return false;

                }
            }

            return true;
        }

        boolean hasConvergedByInspection()
        {
            Map<InetAddress, GossiperSimulator> gossipers = CustomMessagingService.instance().gossipers;
            for (GossiperSimulator simulator : gossipers.values())
            {
                Collection<InetAddress> peerAddrs = new ArrayList<>(gossipers.keySet());

                for (Map.Entry<InetAddress, EndpointState> peer : simulator.endpointStateMap.entrySet())
                {
                    InetAddress peerAddr = peer.getKey();
                    //this case *really* shouldn't fail - would seem to be more of my error than anything else
                    if (!peerAddrs.remove(peerAddr))
                        return false;
                    if (peerAddr.equals(simulator.broadcastAddr))
                        continue;

                    // simulator knows about peer, now let's compare states
                    EndpointState localEndpointState = peer.getValue();
                    EndpointState peerEndpointState = gossipers.get(peerAddr).getEndpointStateForEndpoint(peerAddr);

                    // first compare the heartbeats
                    //NOTE: the heartBeat.version is almost guaranteed to be different (non-convergent), especially in anything larger than a very small cluster,
                    // as the target/source node updates it's heartbeat.version on every gossip round. thus, don't bother to compare them
                    if (localEndpointState.getHeartBeatState().getGeneration() != peerEndpointState.getHeartBeatState().getGeneration())
                    {
                        logger.debug("hasConvergedByInspection: generations are different: local = {}, target = {}",
                                localEndpointState.getHeartBeatState().getGeneration(), peerEndpointState.getHeartBeatState().getGeneration());
                        return false;
                    }

                    // next, compare the app states
                    Collection<ApplicationState> peerAppStates = new ArrayList<>(peerEndpointState.applicationState.keySet());
                    for (Map.Entry<ApplicationState, VersionedValue> localAppStateEntry : localEndpointState.applicationState.entrySet())
                    {
                        ApplicationState appState = localAppStateEntry.getKey();
                        if (!peerAppStates.remove(appState))
                        {
                            logger.debug("hasConvergedByInspection: unknown app state: peer {} does not have AppState {} that local does", new Object[]{peerAddr, appState, simulator.broadcastAddr});
                            return false;
                        }
                        if (localAppStateEntry.getValue().compareTo(peerEndpointState.getApplicationState(appState)) != 0)
                        {
                            logger.debug("hasConvergedByInspection: divergent app state: AppState {} has local({}) version {} and peer({}) version {}",
                                    new Object[]{appState, simulator.broadcastAddr, localAppStateEntry.getValue().value, peerAddr, peerEndpointState.getApplicationState(appState).value});
                            return false;
                        }
                    }
                    if (!peerAppStates.isEmpty())
                    {
                        logger.debug("hasConvergedByInspection: unknown app states: current node {} doesn't know about the following app states from {}: {}", new Object[]{simulator.broadcastAddr, peerAddr, peerAppStates});
                        return false;
                    }
                }

                if (!peerAddrs.isEmpty())
                {
                    if (peerAddrs.size() < 8)
                        logger.debug("hasConvergedByInspection: unknown nodes: current node {} doesn't know about the following nodes: {}", simulator.broadcastAddr, peerAddrs);
                    else
                        logger.debug("hasConvergedByInspection: unknown nodes: current node {} doesn't know about {} nodes (out of {} total)", new Object[]{simulator.broadcastAddr, peerAddrs.size(), gossipers.size()});
                    return false;
                }
            }
            return true;
        }
    }
}
