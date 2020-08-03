package com.gree.util

import java.net.InetAddress

import com.gree.constant.Constant
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

object EsConnection {

  def createESConnection():TransportClient = {
    val settings: Settings = Settings.builder()
      .put("cluster.name", Constant.CLUSTER_NAME)
      .build()

    //连接6台ES
    val client: TransportClient = new PreBuiltTransportClient(settings)
      .addTransportAddresses(
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST209), Constant.ES_PROT),
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST210), Constant.ES_PROT),
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST211), Constant.ES_PROT)
      )
    client
  }

  val conn:TransportClient = createESConnection()

  Runtime.getRuntime.addShutdownHook(new Thread(){
    override def run(): Unit = {
      println("close")
      conn.close()
    }
  })

}

