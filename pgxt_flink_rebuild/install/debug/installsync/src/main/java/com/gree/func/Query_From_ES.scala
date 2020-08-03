package com.gree.func


import com.gree.model.AzKafkaBuidData
import com.gree.util.EsConnection
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks



/**
  * 此类用于查询es是否已经存在该数据
  * 输入：具体索引，主键名称，主键内容，pgguid内容，ts内容，表名称
  * 返回: 主键内容，pgguid内容，具体索引，ts内容，是否存在（Ture/False）
  */


class Query_From_ES extends ProcessFunction[(String,String,String,String,String,String),(AzKafkaBuidData,Boolean)]{

  override def processElement(input: (String,String,String,String,String,String)
                              , ctx: ProcessFunction[(String,String,String,String,String,String),(AzKafkaBuidData,Boolean)]#Context, out: Collector[(AzKafkaBuidData,Boolean)]): Unit = {

      // 建立ES连接
      val client: TransportClient = EsConnection.conn
      val logger: Logger = LoggerFactory.getLogger(Query_From_ES.super.getClass)
    //把数据换成json格式输出后写入kafka。(kafka消费list数据报错)
    val id : String = input._3
    val pgguid : String = input._4
    val table : String = input._6
    val ts : String = input._5
    var isExist : Boolean = false

    val loop = new Breaks
    try{

      loop.breakable {
        for ( i <- 1 to 6) {
          val searchResponse1: SearchResponse = client
            .prepareSearch(input._1)
            .setTypes("_doc")
            .setQuery(QueryBuilders.boolQuery()
              .must(QueryBuilders.termQuery(input._2, input._3))
              .must(QueryBuilders.termQuery("ts", input._5)))
            //QueryBuilders.rangeQuery("ts").gte(input._5)
            .get()

          val hits = searchResponse1.getHits.totalHits
          logger.info("主表查询出的记录数：=>" + hits)


          if (hits != 0) {
            isExist = true
            loop.break()
          }
          Thread.sleep(100)
        }
      }


          out.collect(AzKafkaBuidData(id,pgguid,table,ts),isExist)

      }catch {
      case e:Exception => logger.info("状态判断函数异常："+e.getMessage)
    }

  }
}
