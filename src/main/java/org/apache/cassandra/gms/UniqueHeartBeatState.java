package org.apache.cassandra.gms;

import java.util.concurrent.atomic.AtomicInteger;

class UniqueHeartBeatState extends HeartBeatState
{
    private final AtomicInteger versionGen;
    private int localVersion;

    UniqueHeartBeatState(int gen)
    {
        this(gen, 0);
    }

    UniqueHeartBeatState(int gen, int ver)
    {
        super(gen, ver);
        versionGen = new AtomicInteger(ver);
        this.localVersion = ver;
    }

    void updateHeartBeat()
    {
        localVersion = versionGen.incrementAndGet();
    }

    int getHeartBeatVersion()
    {
        return localVersion;
    }

    public String toString()
    {
        return String.format("UniqueHeartBeat: generation = %d, version = %d", getGeneration(), localVersion);
    }
}
