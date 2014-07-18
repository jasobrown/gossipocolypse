package org.apache.cassandra.gms.helpers;

import org.apache.cassandra.gms.*;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CustomMessagingService
{
    private static final CustomMessagingService INSTANCE = new CustomMessagingService();

    public static CustomMessagingService instance()
    {
        return INSTANCE;
    }

    public static final Map<InetAddress, GossiperSimulator> gossipers = new ConcurrentHashMap<>();

    public static final Map<String, byte[]> parameters = new HashMap<String, byte[]>();

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

    public static void register(GossiperSimulator gossiper)
    {
        gossipers.put(gossiper.broadcastAddr, gossiper);
    }
}
