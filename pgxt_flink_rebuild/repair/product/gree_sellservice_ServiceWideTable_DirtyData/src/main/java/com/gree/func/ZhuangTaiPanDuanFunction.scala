package com.gree.func

import com.gree.model.{DirtyData, Tbl_Assign_Model, TrueData}
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.slf4j.{Logger, LoggerFactory}

class ZhuangTaiPanDuanFunction(zangshuju:OutputTag[DirtyData])  extends  ProcessFunction[Tbl_Assign_Model,TrueData]{
  override def processElement(value: Tbl_Assign_Model, ctx: ProcessFunction[Tbl_Assign_Model, TrueData]#Context, out: Collector[TrueData]): Unit = {
   //进入判断函数先睡眠100毫秒。
    Thread.sleep(100)
    // 建立ES连接
    val client: TransportClient = ESTransportPoolUtil.getClient
    val logger: Logger = LoggerFactory.getLogger(ZhuangTaiPanDuanFunction.super.getClass)
    //根据流数据返查es进行判断,查找是否存在同一个主键id。
    var describe:String = "null"
    var state:Long = 0
    try {
      logger.info("索引名称:"+value.index+"查询主键id名称："+ value.query_name +"查询主键id值："+ value.id + "数据来源表名："+ value.table)
      //如果在es中找不到对应id的值，则说明数据同步到es发生了丢数据
      val zhuangtai:SearchResponse = client
        .prepareSearch(value.index)
        .setTypes("_doc")
        .setQuery(QueryBuilders.termQuery(value.query_name, value.id))
        .get()
      logger.info("主键id查询出的记录数：=>" + zhuangtai.getHits.totalHits)
      if (zhuangtai.getHits.totalHits != 0){
        describe = "ES发生了丢数据，最新状态数据字段丢失，此条数据需要重新处理"
      }else{
        state = 1
        describe = "ES发生了丢数据,并且整条数据丢失，此条数据需要重新处理"
      }
      //在建立二次es连接查询时，睡眠100ms 根据流数据返查es进行判断,查找是否存在同一个主键id，且时间戳大于等于流数据的记录
      Thread.sleep(200)
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
        ctx.output(zangshuju,DirtyData(value.id,value.pgid,value.table,value.ts,state,describe))
      }
    } catch {
      case e: Exception => logger.info("状态判断表状态判断函数异常：" + e.getMessage)
    }

    ESTransportPoolUtil.returnClient(client)
  }
}
