package com.gree.evaluate

import com.gree.constant.Constant
import com.gree.model.{TblWangdianSjdwmx, WangdianLevel}
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}


object BaseLevel {

  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect
    val logger: Logger = LoggerFactory.getLogger(BaseLevel.getClass)

    //任务并行度
    fsEnv.setParallelism(1)

    //分流
    val tblWangdianSjdwmxData = new OutputTag[TblWangdianSjdwmx](Constant.TBL_WANGDIANSJDWMX_DATA)
    val wangdianLevelData = new OutputTag[WangdianLevel](Constant.WD_Level_DATA)


    //消费TblWangdianSjdwmx表数据
    val wangdianSjdwmxJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_WANGDIAN_SJDWMX_TOPIC, fsEnv)
    wangdianSjdwmxJsonData.print("wangdianSjdwmxJsonData->")


    //数据直接输入到TblWangdianSjdwmx表的Sink
    val tblWangdianSjdwmxSink = new ElasticsearchSink.Builder[TblWangdianSjdwmx](
      httpConnect,
      new ElasticsearchSinkFunction[TblWangdianSjdwmx] {
        def process(tblWangdianSjdwmx: TblWangdianSjdwmx, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblWangdianSjdwmxJson(tblWangdianSjdwmx)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_WANGDIAN_SJDWMX)
            .`type`(Constant.ES_TYPE)
            .id(tblWangdianSjdwmx.id)
            .source(json)
          indexer.add(rqst)

          logger.info("发送到app_greeshmasterdata_tbl_wangdian_sjdwmx_v1表es")
        }
      }
    )
    tblWangdianSjdwmxSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    tblWangdianSjdwmxSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    tblWangdianSjdwmxSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    //数据同步分流
    val wdData: DataStream[TblWangdianSjdwmx] = wangdianSjdwmxJsonData.process(new WangDianSjdwmxDataFunction(tblWangdianSjdwmxData))
    wdData.addSink(tblWangdianSjdwmxSink.build())

    //数据逻辑判断
    val quanXianLevelData: DataStream[WangdianLevel] = wdData.getSideOutput[TblWangdianSjdwmx](tblWangdianSjdwmxData)
      .map(tblWangdianSjdwmx => (tblWangdianSjdwmx.id, tblWangdianSjdwmx))
      .process(new QuanXianLevelPanDuanFunction(wangdianLevelData))


    //数据输出到WangdianLevel表的Sink
    val wangdianLevelSink = new ElasticsearchSink.Builder[WangdianLevel](
      httpConnect,
      new ElasticsearchSinkFunction[WangdianLevel] {
        def process(wangdianLevel: WangdianLevel, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getWangdianLevelJson(wangdianLevel)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.WANGDIANL_EVEL)
            .`type`(Constant.ES_TYPE)
            .id(wangdianLevel.id)
            .source(json)
          indexer.add(rqst)

          logger.info("发送到quanxianlevel_wangdianlevel_v1表es")
        }
      }
    )
    wangdianLevelSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    wangdianLevelSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    wangdianLevelSink.setBulkFlushInterval(Constant.TWO_THOUSAND)


    quanXianLevelData.addSink(wangdianLevelSink.build())

    fsEnv.execute("权限层级")


  }
}
