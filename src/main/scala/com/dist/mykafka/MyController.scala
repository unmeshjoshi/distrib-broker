package com.dist.mykafka

import com.dist.simplekafka.ZookeeperClient

class MyController(val zookeeperClient: MyZookeeperClient, val brokerId: Int) {

  def startup(): Unit = {
    elect()
  }

  def elect(): Unit = {

  }
}
