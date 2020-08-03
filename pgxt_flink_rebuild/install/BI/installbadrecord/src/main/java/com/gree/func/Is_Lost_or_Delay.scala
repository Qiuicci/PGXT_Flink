package com.gree.func

import com.gree.model.AzKafkaBuidData
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks

class Is_Lost_or_Delay extends ProcessFunction[AzKafkaBuidData,(AzKafkaBuidData,Int,String)]{

  override def processElement(input:AzKafkaBuidData
                              , ctx: ProcessFunction[AzKafkaBuidData,(AzKafkaBuidData,Int,String)]#Context, out: Collector[(AzKafkaBuidData,Int,String)]): Unit = {
    // 获取ES连接
    val client: TransportClient = ESTransportPoolUtil.getClient
    val logger: Logger = LoggerFactory.getLogger(Is_Lost_or_Delay.super.getClass)
    val id : String = input.id
    val pgguid : String = input.pgguid
    val table : String = input.table
    val ts : Long = input.ts
    var state : Int = 1 //默认丢失数据
    var data_describe = "同步到ES数据丢失，实际无该数据"

    //索引拼接default_server_greeshinstall_  +  表名称  + _t1
    val indexName = "default_server_greeshinstall_" + table + "_t1"





        val searchResponse1: SearchResponse = client
          .prepareSearch(indexName)
          .setTypes("_doc")
          .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("id", input.id))
            //.must(QueryBuilders.termQuery("ts", input.ts)))
            .must(QueryBuilders.rangeQuery("ts").gte(input.ts)))
          .get()
        val hits = searchResponse1.getHits.totalHits
        logger.info("主表查询出的记录数：=>" + hits)

        //判断数据是否是延迟，延迟则不做处理
        if (hits != 0) {
          state = 0 //变成0，0为非处理数据
          data_describe = "同步到ES数据有延迟导致有脏数据，实际数据已经存在"
         // loop.break()
        }
        Thread.sleep(200)





    out.collect(AzKafkaBuidData(id,pgguid,table,ts),state,data_describe)

    ESTransportPoolUtil.returnClient(client)

  }

}
