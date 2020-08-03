package com.gree.async

import com.gree.model.AzKafkaBuidData
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture, RichAsyncFunction}
import org.elasticsearch.action.search.{SearchResponse, SearchType}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks

/**
  * 此类用于查询es是否已经存在该数据
  * 输入：具体索引，主键名称，主键内容，pgguid内容，ts内容，表名称
  * 返回: 主键内容，pgguid内容，具体索引，ts内容，是否存在（Ture/False）
  */

class AsyncFromES extends RichAsyncFunction[(String,String,String,String,String,String),(AzKafkaBuidData,Boolean)]{
  lazy val client: TransportClient = ESTransportPoolUtil.getClient


  override def asyncInvoke(input: (String,String,String,String,String,String),resultFuture: ResultFuture[(AzKafkaBuidData,Boolean)]): Unit = {

    val logger : Logger = LoggerFactory.getLogger(AsyncFromES.super.getClass)
    val id : String = input._3
    val pgguid : String = input._4
    val table : String = input._6
    val ts : String = input._5
    var isExist : Boolean = false

    val loop = new Breaks

    loop.breakable {
      for (i <- 1 to 6) {
      val searchResponse1: SearchResponse = client
        .prepareSearch(input._1)
        .setTypes("_doc")
        .setQuery(QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery(input._2, input._3))
          .must(QueryBuilders.termQuery("ts",input._5)))
          //.must(QueryBuilders.rangeQuery("ts").gte(input._5)))
        //.setSize(0)
        .get()

      //searchResponse1.
      val hits = searchResponse1.getHits.totalHits

        logger.info("主表查询出的记录数：=>" + hits)
      if (hits != 0) {
        isExist = true
        loop.break()
      }
        Thread.sleep(100)


      }
    }


    resultFuture.complete(Iterable((AzKafkaBuidData(id,pgguid,table,ts),isExist)))
  }

  override def close(): Unit ={
    ESTransportPoolUtil.returnClient(client)
  }


}