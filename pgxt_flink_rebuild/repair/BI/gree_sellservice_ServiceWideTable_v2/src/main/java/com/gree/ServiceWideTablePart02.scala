package com.gree



import java.util.Properties

import com.gree.constant.Constant
import com.gree.func.{WeiWanGongChaoShiDeleteMySqlSink, WeiWanGongChaoShiToMySqlSink, _}
import com.gree.model.{KuanBiaoToTidb, MaxKuanBiao, WxDataChaoShi}
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


object ServiceWideTablePart02 {
  def main(args: Array[String]): Unit = {
    System.setProperty("io.netty.allocator.type", "unpooled")
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val logger: Logger = LoggerFactory.getLogger(ServiceWideTablePart02.getClass)

    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //设置checkpoint
    //执行checkpoint的时候，把state的快照数据保存到配置的hdfs文件系统中
    // fsEnv.setStateBackend( new FsStateBackend("hdfs://namenode:port/flink-checkpoints/chk-17"))
    //设置checkpoint执行的间隔  10000ms
   // fsEnv.enableCheckpointing(1000)
    //设置checkpoint数据恢复时，只会执行一次
   // fsEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置每个checkpoint执行的间隔  5000ms
    //fsEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //设置一个checkpoint的完成所需的时间范围 60000ms
   // fsEnv.getCheckpointConfig.setCheckpointTimeout(60000)
    //允许job中同一个时刻执行的checkpoint的数量。 1
    //fsEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置当作业取消的时候删除checkpoint状态数据
    //fsEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION(true))
    //设置checkpoint执行失败时，job是否需要停止
   // fsEnv.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect

    //设置并行度
    fsEnv.setParallelism(6)
    //设置window窗口的时间类型：这里是processing time，已进入窗口计算的时间为基准
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //定义超时数据分流
    val chaoshi =new OutputTag[WxDataChaoShi]("chaoshi")
    val weiwangongchaoshi = new OutputTag[WxDataChaoShi]("weiwangongchaoshi")
    val wangongweichaoshi = new OutputTag[WxDataChaoShi]("wangongweichaoshi")


    val properties = new Properties()
    properties.setProperty("bootstrap.servers",Constant.KAFKA_HOST_LIST)
    properties.setProperty("group.id",Constant.KAFKA_GROUP_ID)
    //消费已完成同步的维修数据WxData-topic
    val wxdata =  KafkaUtil.getKafkaConnect("WxData-T2",fsEnv)
      .map(
          x =>{
            val pgid = x.get("value").get("pgid").asText()
            val ts = x.get("value").get("ts").asLong()
            (pgid, ts)
          }
        )
   // wxdata.print("打印消费的数据：===============")


    // 时间窗口聚合pgid和ts，然后进行业务逻辑计算,组宽表
    val windowdata:DataStream[(String,Long)] = wxdata.keyBy(0)
      .timeWindow(Time.milliseconds(500L))
      .reduce{
        (a,b) => (a._1, if(a._2 > b._2) a._2 else b._2)
      }

    /*//组宽表逻辑--落地到tidb
    val tidbtable:DataStream[KuanBiaoToTidb] = windowdata.process(new TidbKuanBiaoFunction())
    //tidbtable.print("测试打印组成的宽表数据：------------------》")
    //数据写入到tidb中
    tidbtable.addSink(new KuanBiaoToMySqlSink)*/

    //组宽表数据流程
    val maxtable:DataStream[MaxKuanBiao] = windowdata.process(new MaxWxkbFunction())

    //计算超时业务流程
    val wxchaoshi:DataStream[WxDataChaoShi] = windowdata.process(new ChaoShiKanBanFunction(chaoshi,weiwangongchaoshi,wangongweichaoshi))

    //大宽表sink
    val maxKuanBiaoSink = new ElasticsearchSink.Builder[MaxKuanBiao](
      httpConnect,
      new ElasticsearchSinkFunction[MaxKuanBiao] {
        def process(maxKuanBiao: MaxKuanBiao, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getWxkbMaxBigKuanBiaoJson(maxKuanBiao)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.WXKB_MAX_BIG_KUANGBIAO)
            .`type`(Constant.ES_TYPE)
            .id(maxKuanBiao.pgid)
            .source(json)

          indexer.add(rqst)

          logger.info("发送到维修超大宽表")
        }
      }
    )

    maxKuanBiaoSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    maxKuanBiaoSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    maxKuanBiaoSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    maxtable.addSink(maxKuanBiaoSink.build())


    //输出到已完工超时表
    val chaoShiSink = new ElasticsearchSink.Builder[WxDataChaoShi](
      httpConnect,
      new ElasticsearchSinkFunction[WxDataChaoShi] {
        def process(wxDataChaoShi: WxDataChaoShi, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getWxkbTimeoutOrdersJson(wxDataChaoShi)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.WXKB_TIMEOOUT_ORDERS)
            .`type`(Constant.ES_TYPE)
            .id(wxDataChaoShi.pgid)
            .source(json)

          indexer.add(rqst)

          logger.info("发送到超时表es")
        }
      }
    )
    chaoShiSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    chaoShiSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    chaoShiSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    wxchaoshi.getSideOutput(chaoshi).addSink(chaoShiSink.build())


    //未完工工单输入到mysql
    wxchaoshi.getSideOutput(weiwangongchaoshi).addSink(new WeiWanGongChaoShiToMySqlSink)
    //完成工单从mysql删除
    wxchaoshi.getSideOutput(wangongweichaoshi).addSink(new WeiWanGongChaoShiDeleteMySqlSink)



    fsEnv.execute("维修数据业务处理部分")
  }
}
