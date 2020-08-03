package com.gree.util

import java.net.InetAddress

import com.gree.constant.Constant
import org.apache.http.HttpHost
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

object EsUtil extends  Serializable {
  val conf = ConfigurationUtil(Constant.CONFIG_PROPERTIES)

  def getEsConnect: TransportClient = {
    val settings: Settings = Settings.builder()
      .put("cluster.name", Constant.CLUSTER_NAME)
      .build()

    //连接ES集群
    val client: TransportClient = new PreBuiltTransportClient(settings)
      .addTransportAddresses(
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST209), Constant.ES_PROT),
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST210),Constant.ES_PROT),
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST211),Constant.ES_PROT),
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST212),Constant.ES_PROT),
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST213),Constant.ES_PROT),
        new TransportAddress(InetAddress.getByName(Constant.ES_HOST214),Constant.ES_PROT)
      )
    client
  }

  /**
   * 获取ES http连接
   */
  def getEsHttpConnect:java.util.ArrayList[HttpHost]={
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost(Constant.ES_HOST209, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
    httpHosts.add(new HttpHost(Constant.ES_HOST210, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
    httpHosts.add(new HttpHost(Constant.ES_HOST211, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
    httpHosts.add(new HttpHost(Constant.ES_HOST212, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
    httpHosts.add(new HttpHost(Constant.ES_HOST213, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
    httpHosts.add(new HttpHost(Constant.ES_HOST214, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
    httpHosts
  }
}