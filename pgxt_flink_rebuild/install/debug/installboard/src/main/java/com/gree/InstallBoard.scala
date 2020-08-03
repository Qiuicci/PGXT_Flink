package com.gree

import com.gree.constant.Constant
import com.gree.func.{ Max_Az_Order_Func, MyActionRequestFailureHandler, Out_of_Time_Func, WeiWanGongChaoShiDeleteMySqlSink, WeiWanGongChaoShiToMySqlSink}
import com.gree.model.{AZMaxOrder, AzDataChaoShi}
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{ OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}

object InstallBoard {
  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect
    val logger: Logger = LoggerFactory.getLogger(InstallBoard.getClass)

    //任务并行度
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //分流
    val chaoshi = new OutputTag[AzDataChaoShi](Constant.CHAO_SHI)
    val weiwangongchaoshi = new OutputTag[AzDataChaoShi](Constant.WEI_WAN_GONG_CHAO_SHI)
    val wangongweichaoshi = new OutputTag[AzDataChaoShi](Constant.WAN_GONG_WEI_CHAO_SHI)


    /**
     * 从kafka的AzData-topic中抽取数据
     */
    val azDataJson_Kafka = KafkaUtil.getKafkaConnect("AzData-goodRecord",fsEnv)
      .map(a =>  new Tuple3[String,String,Long](a.get("value").get("pgguid").asText(),a.get("value").get("table").asText(), a.get("value").get("ts").asLong()))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .max(2)
    //时间窗口流
    //val  azGoodRecord = azDataJson_Kafka.map(a =>  new Tuple3[String,String,Long](a.get("value").get("pgguid").asText(),a.get("value").get("table").asText(), a.get("value").get("ts").asLong()))
     /* .keyBy(1).keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .max(2)*/

    //azDataJson_Kafka.print("消费到的azdata-goodrecord ->")
    /**
     * 超时的逻辑函数，Out_of_Time_Func
     */
    val outOfTime = azDataJson_Kafka
      .process(new Out_of_Time_Func(chaoshi, weiwangongchaoshi, wangongweichaoshi))

    //输出到超时表数据
    val csSink = new ElasticsearchSink.Builder[AzDataChaoShi](
      httpConnect,
      new ElasticsearchSinkFunction[AzDataChaoShi] {
        def process(azDataChaoShi: AzDataChaoShi, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getAzkbTimeoutOrdersJson(azDataChaoShi)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.AZKB_TIMEOUT_ORDER)
            .`type`(Constant.ES_TYPE)
            .id(azDataChaoShi.pgguid)
            .source(json)
          indexer.add(rqst)
          logger.info("安装发送到超时表es")
        }
      }
    )

    //输出到超时es数据
    outOfTime.getSideOutput[AzDataChaoShi](chaoshi).addSink(csSink.build())
    //输出到mysql未完工，未超时表
    outOfTime.getSideOutput[AzDataChaoShi](weiwangongchaoshi).addSink(new WeiWanGongChaoShiToMySqlSink)
    //删除mysql的未完工表
    outOfTime.getSideOutput[AzDataChaoShi](wangongweichaoshi).addSink(new WeiWanGongChaoShiDeleteMySqlSink)
    /**
     * 最大的安装api输出
     */
    val maxAzOrder = azDataJson_Kafka
      .process(new Max_Az_Order_Func)

    maxAzOrder.print("**********************最大宽表输出*******************->")
    /////////////////////将最大的index输出//////////////////////////////////////////////
    //数据直接输AZmaxorder的Sink
    val azMaxKuanBiaoSink = new ElasticsearchSink.Builder[AZMaxOrder](
      httpConnect,
      new ElasticsearchSinkFunction[AZMaxOrder] {
        def process(azmaxOrder: AZMaxOrder, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getAzkbMaxBigKuanBiaoJson(azmaxOrder)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.AZKB_MAX_BIG_KUANBIAO)
            .`type`(Constant.ES_TYPE)
            .id(azmaxOrder.pgguid)
            .source(json)
          indexer.add(rqst)

          logger.info("发送到安装MAX宽表es")
        }
      }
    )
    azMaxKuanBiaoSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azMaxKuanBiaoSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azMaxKuanBiaoSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    azMaxKuanBiaoSink.setFailureHandler(new MyActionRequestFailureHandler())
    //数据直接输AZmaxorder的Sink
    maxAzOrder.addSink(azMaxKuanBiaoSink.build())

    fsEnv.execute("安装看板重构版1.0")



  }

}
