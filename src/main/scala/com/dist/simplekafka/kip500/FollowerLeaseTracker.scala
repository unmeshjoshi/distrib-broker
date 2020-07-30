package com.dist.simplekafka.kip500

import java.util.concurrent.ConcurrentHashMap

class FollowerLeaseTracker(var leases: ConcurrentHashMap[String, Lease]) extends LeaseTracker {
  override def expireLeases(): Unit = {
  }

  override def addLease(lease: Lease): Unit = {
    leases.put(lease.getName, lease)
  }

  override def start(): Unit = {
  }

  override def stop(): Unit = {
  }

  override def refreshLease(name: String): Unit = {
  }

  override def revokeLease(name: String): Unit = {
    leases.remove(name)
  }
}
