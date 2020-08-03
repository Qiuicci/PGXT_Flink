package com.gree.func

import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, RequestIndexer}
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.client.transport.TransportClient
import org.slf4j.{Logger, LoggerFactory}


class MyActionRequestFailureHandler extends ActionRequestFailureHandler{

  override def onFailure(action: ActionRequest, failure: Throwable, restStatusCode: Int, indexer: RequestIndexer): Unit = {
    val logger: Logger = LoggerFactory.getLogger(MyActionRequestFailureHandler.super.getClass)
    val client: TransportClient = ESTransportPoolUtil.getClient

    val aaa: String = action.toString
    val message = failure.getLocalizedMessage
    logger.info("数据为->"+aaa)
    println("数据为->"+aaa)
    logger.info("失败原因->"+failure.getCause)
    println("失败原因->"+failure.getCause)
    logger.info("失败信息"+failure.getMessage)
    println("失败信息"+failure.getMessage)
    logger.info("getLocalizedMessage"+failure.getLocalizedMessage)
    println("getLocalizedMessage"+failure.getLocalizedMessage)

    val source = message.indexOf("id")
    val pgguid = message.substring(source + 4, message.length - 2)

    logger.info("sink到azkb_max_big_kuanbiao异常的pgguid为->"+pgguid)

    val builder: XContentBuilder = XContentFactory.jsonBuilder().startObject()
      .field("pgguid", pgguid)
      .field("信息描述",aaa)
      .field("失败原因", failure.getCause.toString)
      .field("失败信息", failure.getMessage)
      .field("getLocalizedMessage", failure.getLocalizedMessage)
      .endObject()

    client.prepareIndex("azkb_max_big_error_message", "_doc",pgguid).setSource(builder).get()
    ESTransportPoolUtil.returnClient(client)
  }
}
