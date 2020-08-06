package com.dist.simplekafka.kip500

import java.util.concurrent.ConcurrentHashMap

import com.dist.simplekafka.kip500.network.Config

import scala.jdk.CollectionConverters._

class LeaderLeaseTracker(config:Config, var leases: ConcurrentHashMap[Int, Lease], var clock: SystemClock, var server: Consensus) extends Logging with LeaseTracker {
  val now: Long = clock.nanoTime
  this.leases.values.forEach((l: Lease) => {
      l.refresh(now)
  })
  info("Creating leader tracker with " + this.leases)
  private val scheduler = new HeartBeatScheduler(this.expireLeases)

  override def expireLeases() = {
    val now = System.nanoTime
    val leaseIds = leases.keySet
    for (leaseId <- leaseIds.asScala) {
      val lease = leases.get(leaseId)
      if (lease.getExpirationTime < now) {
        info("Revoking lease with id " + lease.getName + " after " + lease.expirationTime + " in " + config.serverId)
        leases.remove(leaseId)
        server.propose(FenceBroker(lease.getName))
      }
    }
  }

  override def addLease(lease: Lease): Unit = {
    leases.put(lease.getName, lease)
    lease.refresh(clock.nanoTime)
  }

  override def start(): Unit = {
    scheduler.start()
  }

  override def stop(): Unit = {
    scheduler.cancel()
  }

  override def refreshLease(name: String): Unit = {
    leases.get(name).refresh(clock.nanoTime)
  }

  override def revokeLease(name: String): Unit = {
    leases.remove(name)
  }
}
