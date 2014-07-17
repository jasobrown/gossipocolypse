package net.jasondev.gossipocolypse.cassandra.helpers;

import net.jasondev.gossipocolypse.cassandra.GossipDigestSyn;
import org.apache.cassandra.net.MessageOut;

import java.net.InetAddress;

public class CustomMessagingService
{
    private static final CustomMessagingService INSTANCE = new CustomMessagingService();

    public static CustomMessagingService instance()
    {
        return INSTANCE;
    }

    public void sendOneWay(MessageOut<GossipDigestSyn> message, InetAddress to)
    {

    }
}
