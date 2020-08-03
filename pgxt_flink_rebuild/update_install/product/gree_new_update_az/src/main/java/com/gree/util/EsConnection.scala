package com.gree.util

import java.net.InetAddress
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

object EsConnection {

  def createESConnection():TransportClient = {
    val settings: Settings = Settings.builder()
      .put("cluster.name", "gree_elasticsearch")
      .build()

    //连接6台ES
    val client: TransportClient = new PreBuiltTransportClient(settings)
      .addTransportAddresses(
        new TransportAddress(InetAddress.getByName("10.7.17.214"), 9300),
        new TransportAddress(InetAddress.getByName("10.7.17.213"), 9300),
        new TransportAddress(InetAddress.getByName("10.7.17.212"), 9300),
        new TransportAddress(InetAddress.getByName("10.7.17.211"), 9300),
        new TransportAddress(InetAddress.getByName("10.7.17.210"), 9300),
        new TransportAddress(InetAddress.getByName("10.7.17.209"), 9300)
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
