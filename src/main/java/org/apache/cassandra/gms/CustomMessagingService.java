package org.apache.cassandra.gms;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

public class CustomMessagingService
{
    public static final Map<String, byte[]> parameters = new HashMap<String, byte[]>();

    private static CustomMessagingService INSTANCE = new CustomMessagingService();

    public final Map<InetAddress, GossiperSimulator> gossipers = new ConcurrentHashMap<>();
    private final Random random;

    public CustomMessagingService()
    {
        random = new Random(System.nanoTime());
    }

    public static CustomMessagingService instance()
    {
        return INSTANCE;
    }

    //should only be called at the beginning on a simulation
    public static void renewInstance()
    {
        INSTANCE = new CustomMessagingService();
    }

    public void sendOneWay(MessageOut message, InetAddress to, GossiperSimulator sender)
    {
        GossiperSimulator target = gossipers.get(to);
        if (target == null)
            throw new IllegalArgumentException("unknown peer addr: " + to);

        switch (message.verb)
        {
            case GOSSIP_DIGEST_SYN:
                MessageIn<GossipDigestSyn> synMsg = MessageIn.create(message.from, (GossipDigestSyn)message.payload, parameters, message.verb, 0);
                new GossipDigestSynVerbHandlerSimulator().doVerb(synMsg, sender, target);
                break;
            case GOSSIP_DIGEST_ACK:
                MessageIn<GossipDigestAck> ackMsg = MessageIn.create(message.from, (GossipDigestAck)message.payload, parameters, message.verb, 0);
                new GossipDigestAckVerbHandlerSimulator().doVerb(ackMsg, sender, target);
                break;
            case GOSSIP_DIGEST_ACK2:
                MessageIn<GossipDigestAck2> msg = MessageIn.create(message.from, (GossipDigestAck2)message.payload, parameters, message.verb, 0);
                new GossipDigestAck2VerbHandlerSimulator().doVerb(msg, target);
                break;
        }
    }

    private void generateDelay()
    {
        // would love to do some neato probability distributions, but, alas, I'm not smart enough <sigh>
        double d = random.nextDouble();
        long delay = (long)(10000L * d);
        Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MICROSECONDS);
    }


    public void register(GossiperSimulator gossiper)
    {
        gossipers.put(gossiper.broadcastAddr, gossiper);
    }
}
