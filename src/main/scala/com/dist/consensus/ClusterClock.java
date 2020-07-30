package com.dist.consensus;

public class ClusterClock {

    private SystemClock clock;

    public ClusterClock(SystemClock clock) {
        this.clock = clock;
    }

    /**
     * Invariant: this is equal to the cluster time in:
     * - the last log entry, if any, or
     * - the last snapshot, if any, or
     * - 0.
     */
    long clusterTimeAtEpoch;

    /**
     * The local SteadyClock time when clusterTimeAtEpoch was set.
     */
    long localTimeAtEpoch;

    /**
     * Reset to the given cluster time, assuming it's the current time right
     * now. This is used, for example, when a follower gets a new log entry
     * (which includes a cluster time) from the leader.
     */
    public void newEpoch(long clusterTime) {
        clusterTimeAtEpoch = clusterTime;
        localTimeAtEpoch = clock.nanoTime();
    }

    /**
     * Called by leaders to generate a new cluster time for a new log entry.
     * This is equivalent to the following but slightly more efficient:
     *   uint64_t now = c.interpolate();
     *   c.newEpoch(now);
     *   return now;
     */
    public long leaderStamp() {
        long localTime = clock.nanoTime();
        long nanosSinceEpoch = localTime - localTimeAtEpoch;
        clusterTimeAtEpoch += nanosSinceEpoch;
        localTimeAtEpoch = localTime;
        return clusterTimeAtEpoch;
    }

    public long interpolate() {
        long localTime = clock.nanoTime();
        long nanosSinceEpoch = localTime - localTimeAtEpoch;
        return clusterTimeAtEpoch + nanosSinceEpoch;
    }
}
