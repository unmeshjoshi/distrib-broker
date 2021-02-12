package com.dist.simplekafka

object FindCoordinatorRequest {
  val GROUP_COORDINATOR = "Group"
  val TRANSACTION_COORDINATOR = "Transaction"
}
case class FindCoordinatorRequest(key:String, coordinatorType: String)
