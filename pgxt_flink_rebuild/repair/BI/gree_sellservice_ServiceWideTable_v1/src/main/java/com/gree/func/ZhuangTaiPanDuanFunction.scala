package com.gree.func

import com.gree.model.{Tbl_Assign_Model, TrueData}
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.slf4j.{Logger, LoggerFactory}

class ZhuangTaiPanDuanFunction(zangshuju:OutputTag[TrueData])  extends  ProcessFunction[Tbl_Assign_Model,TrueData]{
  override def processElement(value: Tbl_Assign_Model, ctx: ProcessFunction[Tbl_Assign_Model, TrueData]#Context, out: Collector[TrueData]): Unit = {
    // 建立ES连接
    val client: TransportClient = ESTransportPoolUtil.getClient
    val logger: Logger = LoggerFactory.getLogger(ZhuangTaiPanDuanFunction.super.getClass)
    //根据流数据返查es进行判断,查找是否存在同一个pgguid，且时间戳大于等于流数据的记录
    //logger.info("打印需要查询的索引："+value.index+"+ 以及查询id名称"+value.query_name)
    try {
      val zhubiao: SearchResponse = client
        .prepareSearch(value.index)
        .setTypes("_doc")
        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery(value.query_name, value.id))
          .must(QueryBuilders.rangeQuery("ts").gte(value.ts)))
        .get()
      val fankuiHit: SearchHits = zhubiao.getHits
      logger.info("查询出的记录数：=>" + fankuiHit.totalHits)
      //判断流数据能否从主表es查询出数据，能则进入主流，否则进入侧输出流
      if (fankuiHit.totalHits != 0) {
        out.collect(TrueData(value.id,value.pgid,value.table,value.ts))
      } else {
        ctx.output(zangshuju,TrueData(value.id,value.pgid,value.table,value.ts))
      }
    } catch {
      case e: Exception => logger.info("状态判断表状态判断函数异常：" + e.getMessage)
    }
    ESTransportPoolUtil.returnClient(client)
  }
}
