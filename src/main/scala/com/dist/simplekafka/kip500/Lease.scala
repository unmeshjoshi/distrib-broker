package com.dist.simplekafka.kip500

case class Lease(var brokerId: Int, var ttl: Long) extends Logging {
  private[kip500] var expirationTime = 0L

  def getName = brokerId

  def getTtl: Long = ttl

  def getExpirationTime: Long = expirationTime

  def refresh(now: Long): Unit = {
    expirationTime = now + ttl
    info(s"Refreshing lease ${brokerId} Expiration time is ${expirationTime}")
  }
}
