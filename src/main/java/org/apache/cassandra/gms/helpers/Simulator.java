package org.apache.cassandra.gms.helpers;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.GossiperSimulator;
import org.apache.cassandra.gms.VersionedValue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Simulator
{
    public static void main(String[] args) throws UnknownHostException
    {
        String cwd = System.getProperty("user.dir");
        System.setProperty("logback.configurationFile", "src/main/resources/logback.xml");
        System.setProperty("cassandra.config", "file://" + cwd + "/src/main/resources/cassandra.yaml");

        int seedCnt = 3;
        List<InetAddress> seeds = new ArrayList<>(seedCnt);
        for (int i = 0; i < seedCnt; i++)
        {
            seeds.add(getInetAddr(i));
        }

        int nodeCnt = 21;
        for (int i = 0; i < nodeCnt; i++)
        {
            InetAddress addr = getInetAddr(i);
            GossiperSimulator simulator = new GossiperSimulator(addr, seeds);
            CustomMessagingService.register(simulator);

            Map<ApplicationState, VersionedValue> appStates = new HashMap<>();
            appStates.put(ApplicationState.NET_VERSION, GossiperSimulator.valueFactory.networkVersion());
            appStates.put(ApplicationState.HOST_ID, GossiperSimulator.valueFactory.hostId(UUID.randomUUID()));
            appStates.put(ApplicationState.RPC_ADDRESS, GossiperSimulator.valueFactory.rpcaddress(addr));
            appStates.put(ApplicationState.RELEASE_VERSION, GossiperSimulator.valueFactory.releaseVersion());
            simulator.start(0, appStates);
        }

        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.SECONDS);
    }

    static InetAddress getInetAddr(int i) throws UnknownHostException
    {
        int thirdOctet = i / 255;
        int fourthOctet = i % 255;
        String ipAddr = "127.0." + thirdOctet + "." + fourthOctet;
        return InetAddress.getByName(ipAddr);
    }
}
