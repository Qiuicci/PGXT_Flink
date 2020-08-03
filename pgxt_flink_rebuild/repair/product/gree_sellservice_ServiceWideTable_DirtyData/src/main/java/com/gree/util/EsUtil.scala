package com.gree.util

import java.net.InetAddress

import com.gree.constant.Constant
import org.apache.http.HttpHost
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * ES工具类
  */
object EsUtil extends  Serializable {
  val conf = ConfigurationUtil(Constant.CONFIG_PROPERTIES)
  val httpHosts = new java.util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost(Constant.ES_HOST209, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  httpHosts.add(new HttpHost(Constant.ES_HOST210, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  httpHosts.add(new HttpHost(Constant.ES_HOST211, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  httpHosts.add(new HttpHost(Constant.ES_HOST212, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  httpHosts.add(new HttpHost(Constant.ES_HOST213, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  httpHosts.add(new HttpHost(Constant.ES_HOST214, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))


  /**
    * 获取ES http连接
    */
  def getEsHttpConnect:java.util.ArrayList[HttpHost]={

    httpHosts
  }
}

