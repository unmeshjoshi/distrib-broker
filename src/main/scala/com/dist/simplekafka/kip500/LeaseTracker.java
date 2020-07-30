package com.dist.simplekafka.kip500;

public interface LeaseTracker {
    void expireLeases();

    void addLease(Lease lease);

    void start();

    void stop();

    void refreshLease(String name);

    void revokeLease(String name);
}
