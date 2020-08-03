package com.gree



import java.util.Properties

import com.gree.constant.Constant
import com.gree.func.{ChaoShiKanBanFunction, MaxWxkbFunction, _}
import com.gree.model._
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object ServiceWideTableDirtyData {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(ServiceWideTableDirtyData.getClass)
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
    fsEnv.setParallelism(3)
    //设置window窗口的时间类型：这里是processing time，已进入窗口计算的时间为基准
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //定义超时数据分流
    val zangshuju = new OutputTag[DirtyData]("zangshuju")
    val chaoshi =new OutputTag[WxDataChaoShi]("chaoshi")
    val weiwangongchaoshi = new OutputTag[WxDataChaoShi]("weiwangongchaoshi")
    val wangongweichaoshi = new OutputTag[WxDataChaoShi]("wangongweichaoshi")

    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers",Constant.KAFKA_HOST_LIST)
    properties.setProperty("group.id",Constant.KAFKA_GROUP_ID)*/
//    properties.setProperty("auto.offset.reset",Constant.KAFKA_AUTO_OFFSET_RESET)
    //消费维修的脏数据,根据脏数据值，传入对应的es中index和id便于自动判断
    val falsedata =  KafkaUtil.getKafkaConnect("WxData-F2",fsEnv)
    .map(
          x => {
            val index:String = x.get("value").get("table").asText() match {
              case "tbl_assign" => Constant.TBL_ASSIGN_INDEX
              case "tbl_assign_appointment" => Constant.TBL_ASSIGN_APPOINTMENT_INDEX
              case "tbl_assign_fkmx" => Constant.TBL_ASSIGN_FKMX_INDEX
              case "tbl_assign_mx" => Constant.TBL_ASSIGN_MX_INDEX
              case "tbl_assign_feedback" => Constant.TBL_ASSIGN_FEEDBACK_INDEX
              case "tbl_assign_satisfaction" => Constant.TBL_ASSIGN_SATISFACTION_INDEX
              case "tbl_assign_daijian" => Constant.TBL_ASSIGN_DAIJIAN_INDEX
              case "tbl_assign_xzyd" => Constant.TBL_ASSIGN_XZYD_INDEX
            }
            val query_name:String = x.get("value").get("table").asText() match {
              case "tbl_assign" => Constant.TBL_ASSIGN_QUERY_ID
              case "tbl_assign_appointment" => Constant.TBL_ASSIGN_APPOINTMENT_QUERY_ID
              case "tbl_assign_fkmx" => Constant.TBL_ASSIGN_FKMX_QUERY_ID
              case "tbl_assign_mx" => Constant.TBL_ASSIGN_MX_QUERY_ID
              case "tbl_assign_feedback" => Constant.TBL_ASSIGN_FEEDBACK_QUERY_ID
              case "tbl_assign_satisfaction" => Constant.TBL_ASSIGN_SATISFACTION_QUERY_ID
              case "tbl_assign_daijian" => Constant.TBL_ASSIGN_DAIJIAN_QUERY_ID
              case "tbl_assign_xzyd" => Constant.TBL_ASSIGN_XZYD_QUERY_ID
            }
            val id:String = x.get("value").get("id").asText()
            val pgid:String = x.get("value").get("pgid").asText()
            val table:String = x.get("value").get("table").asText()
            val ts:String = x.get("value").get("ts").asText()

            Tbl_Assign_Model(index,query_name,id,pgid,table,ts)
          }
        )

    val ZhuangTaiPanDuan:DataStream[TrueData] = falsedata.process( new ZhuangTaiPanDuanFunction(zangshuju))
    //错误数据写入到tidb中记录查看数据错误信息
    ZhuangTaiPanDuan.getSideOutput(zangshuju).addSink(new DirtyDataToTidbSink)

    val wxdata:DataStream[(String,Long)] =  ZhuangTaiPanDuan.map(
      x => {
        val pgid:String = x.pgid
        val ts:Long = x.ts.toLong
        (pgid,ts)
      }
    )
    // 时间窗口聚合pgid和ts，然后进行业务逻辑计算,组宽表
    val windowdata:DataStream[(String,Long)] = wxdata.keyBy(0)
      .timeWindow(Time.seconds(1))
      .reduce{
        (a,b) => (a._1, if(a._2 > b._2) a._2 else b._2)
      }
    //组宽表逻辑--落地到tidb
   // val tidbtable:DataStream[KuanBiaoToTidb] = windowdata.process(new TidbKuanBiaoFunction())
    //数据写入到tidb中
   // tidbtable.addSink(new KuanBiaoToMySqlSink)



    //组宽表数据流程
    val maxtable:DataStream[MaxKuanBiao] = windowdata.process(new MaxWxkbFunction())

    //计算超时业务流程
    val wxchaoshi:DataStream[WxDataChaoShi] = windowdata.process(new ChaoShiKanBanFunction(chaoshi,weiwangongchaoshi,wangongweichaoshi))

    //大宽表sink
    val maxKuanBiaoSink = new ElasticsearchSink.Builder[MaxKuanBiao](
      httpConnect,
      new ElasticsearchSinkFunction[MaxKuanBiao] {
        def process(maxKuanBiao: MaxKuanBiao,
         ctx: RuntimeContext, indexer: RequestIndexer) {
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

    maxKuanBiaoSink.setBulkFlushMaxActions(5)
    maxKuanBiaoSink.setBulkFlushMaxSizeMb(5)
    maxKuanBiaoSink.setBulkFlushInterval(1)
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
    chaoShiSink.setBulkFlushMaxActions(5)
    chaoShiSink.setBulkFlushMaxSizeMb(5)
    chaoShiSink.setBulkFlushInterval(1)
    wxchaoshi.getSideOutput(chaoshi).addSink(chaoShiSink.build())


    //未完工工单输入到mysql
    wxchaoshi.getSideOutput(weiwangongchaoshi).addSink(new WeiWanGongChaoShiToMySqlSink)
    //完成工单从mysql删除
    wxchaoshi.getSideOutput(wangongweichaoshi).addSink(new WeiWanGongChaoShiDeleteMySqlSink)



    fsEnv.execute("维修延时数据处理模块")
  }
}
