package com.dist.simplekafka.kip500

case class Lease(var name: String, var ttl: Long) extends Logging {
  private[kip500] var expirationTime = 0L

  def getName: String = name

  def getTtl: Long = ttl

  def getExpirationTime: Long = expirationTime

  def refresh(now: Long): Unit = {
    expirationTime = now + ttl
    info(s"Refreshing lease ${name} Expiration time is ${expirationTime}")
  }
}
