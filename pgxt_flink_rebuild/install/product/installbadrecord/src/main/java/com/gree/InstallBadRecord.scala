package com.gree

import com.gree.constant.Constant
import com.gree.func.{Bad_Record_Into_Mysql, Is_Lost_or_Delay, Max_Az_Order_Func, MyActionRequestFailureHandler, Out_of_Time_Func, WeiWanGongChaoShiDeleteMySqlSink, WeiWanGongChaoShiToMySqlSink}
import com.gree.model.{AZMaxOrder, AzDataChaoShi, AzKafkaBuidData}
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}

object InstallBadRecord {
  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect
    val logger: Logger = LoggerFactory.getLogger(InstallBadRecord.getClass)

    //任务并行度
    fsEnv.setParallelism(3)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //分流
    val chaoshi = new OutputTag[AzDataChaoShi](Constant.CHAO_SHI)
    val weiwangongchaoshi = new OutputTag[AzDataChaoShi](Constant.WEI_WAN_GONG_CHAO_SHI)
    val wangongweichaoshi = new OutputTag[AzDataChaoShi](Constant.WAN_GONG_WEI_CHAO_SHI)

    //抽取安装kafka的脏数据
    val azBadDataJson_Kafka = KafkaUtil.getKafkaConnect("AzData-badRecord",fsEnv)
      .map(a =>  new AzKafkaBuidData(a.get("value").get("id").asText(),a.get("value").get("pgguid").asText(),a.get("value").get("table").asText(), a.get("value").get("ts").asLong()))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .max(2)


    /**
      * 脏数据进入tidb
      */
    val azBadData = azBadDataJson_Kafka.process(new Is_Lost_or_Delay)
    //输入到mysql
    azBadData.addSink(new Bad_Record_Into_Mysql)

    /**
      * 最大的安装输出
      */
    val maxAzOrder = azBadDataJson_Kafka.map(a => new Tuple3(a.pgguid,a.table,a.ts))
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



    /**
      * 超时的逻辑函数，Out_of_Time_Func
      */
    val outOfTime = azBadDataJson_Kafka.map(a => new Tuple3(a.pgguid,a.table,a.ts)).process(new Out_of_Time_Func(chaoshi, weiwangongchaoshi, wangongweichaoshi))

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

    fsEnv.execute("安装消费脏数据重构版本1.0")
  }
}
