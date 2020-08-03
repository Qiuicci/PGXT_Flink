package com.gree.util



import com.gree.constant.Constant
import org.apache.http.HttpHost

object EsUtil extends  Serializable {
  val conf = ConfigurationUtil(Constant.CONFIG_PROPERTIES)
  val httpHosts = new java.util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost(Constant.ES_HOST209, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  httpHosts.add(new HttpHost(Constant.ES_HOST210, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  httpHosts.add(new HttpHost(Constant.ES_HOST211, Constant.ES_HTTP_PROT, Constant.ES_SCHEME))
  /**
   * 获取ES http连接
   */
  def getEsHttpConnect:java.util.ArrayList[HttpHost]={
    httpHosts
  }
}