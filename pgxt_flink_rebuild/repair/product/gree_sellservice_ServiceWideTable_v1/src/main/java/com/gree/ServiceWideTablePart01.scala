package com.gree

import com.esotericsoftware.kryo.serializers.FieldSerializer.Optional
import com.gree.constant.Constant
import com.gree.func.{FkMingXiBiaoDataFunction, ManYiBiaoDataFunction, MingXiBiaoDataFunction, XxxDataFunction, _}
import com.gree.model._
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}

object ServiceWideTablePart01 {
  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(ServiceWideTablePart01.getClass)

    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置checkpoint
    //执行checkpoint的时候，把state的快照数据保存到配置的hdfs文件系统中
    // fsEnv.setStateBackend( new FsStateBackend("hdfs://namenode:port/flink-checkpoints/chk-17"))
    //设置checkpoint执行的间隔  10000ms
    //fsEnv.enableCheckpointing(1000)
    //设置checkpoint数据恢复时，只会执行一次
    //fsEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置每个checkpoint执行的间隔  5000ms
    //fsEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //设置一个checkpoint的完成所需的时间范围 60000ms
    //fsEnv.getCheckpointConfig.setCheckpointTimeout(60000)
    //允许job中同一个时刻执行的checkpoint的数量。 1
    //fsEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置当作业取消的时候删除checkpoint状态数据
    //fsEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION(true))
    //设置checkpoint执行失败时，job是否需要停止
    //fsEnv.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect
    // 定义第一次分流，然后分流数据进入函数判断数据是否已经同步到es
    val modeldata = new OutputTag[Tbl_Assign_Model]("modeldata")
    //定义第二次分流，正确数据进入对应业务域topic，分流数据进入脏数据topic
    val zangshuju = new OutputTag[TrueData]("zangshuju")


    //设置任务并行度
    fsEnv.setParallelism(3)


    //数据直接sink到主表的es中
    val esSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign] {
        def process(tbl_assign: Tbl_Assign, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil.getTblAssignJson(tbl_assign)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign.pgid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到主表ES")
        }
      }
    )

    esSinkBuilder.setBulkFlushMaxActions(5)
    esSinkBuilder.setBulkFlushMaxSizeMb(5)
    esSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    esSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())
    //消费主表数据
    val az_assignJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_TOPIC, fsEnv)
    //az_assignJsonData.print("消费主表数据==============》")

    val ZBData: DataStream[Tbl_Assign] = az_assignJsonData.process(new ZhuBiaoDataFunction(modeldata))
    ZBData.addSink(esSinkBuilder.build())
   // ZBData.print("打印主流输出数据:==========》")
   // ZBData.getSideOutput(modeldata).print("打印输出的测流数据：------》")


    //将预约表数据sink到ES
    val yuyuebiaoSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign_Appointment](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign_Appointment] {
        def process(tbl_assign_appoinment: Tbl_Assign_Appointment, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil. getTblAssignAppointmentJson(tbl_assign_appoinment)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_APPOINTMENT_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign_appoinment.id)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到预约表ES")
        }
      }
    )

    yuyuebiaoSinkBuilder.setBulkFlushMaxActions(5)
    yuyuebiaoSinkBuilder.setBulkFlushMaxSizeMb(5)
    yuyuebiaoSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    yuyuebiaoSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())

    //消费预约表数据
    val assign_appoinmentJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_APPOINTMENT_TOPIC, fsEnv)
    //assign_appoinmentJsonData.print("assign_appoinmentJsonData =>")

    val YuYueData: DataStream[Tbl_Assign_Appointment] = assign_appoinmentJsonData.process(new YuYueBiaoDataFunction(modeldata))
    YuYueData.addSink(yuyuebiaoSinkBuilder.build())


    //将待件表写到ES中
    val daijianSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign_DaiJian](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign_DaiJian] {
        def process(tbl_assign_daijian: Tbl_Assign_DaiJian, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil. getTblAssignDaijian(tbl_assign_daijian)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_DAIJIAN_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign_daijian.id)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到待件表ES")
        }
      }
    )

    daijianSinkBuilder.setBulkFlushMaxActions(5)
    daijianSinkBuilder.setBulkFlushMaxSizeMb(5)
    daijianSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    daijianSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())

    //消费待件表数据
    val assign_daijianJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_DAIJIAN_TOPIC, fsEnv)
    //assign_daijianJsonData.print("assign_daijianJsonData =>")
    val DaiJianData: DataStream[Tbl_Assign_DaiJian] = assign_daijianJsonData.process(new DaiJianBiaoDataFunction(modeldata))
    DaiJianData.addSink(daijianSinkBuilder.build())


    //反馈表数据同步到ES
    val fankuiSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign_FeedBack](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign_FeedBack] {
        def process(tbl_assign_fankui: Tbl_Assign_FeedBack, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil. getTblAssignFeedBack(tbl_assign_fankui)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_FEEDBACK_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign_fankui.id)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到反馈表ES")
        }
      }
    )

    fankuiSinkBuilder.setBulkFlushMaxActions(5)
    fankuiSinkBuilder.setBulkFlushMaxSizeMb(5)
    fankuiSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    fankuiSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())

    //消费反馈表数据
    val assign_fankuiJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_FEEDBACK_TOPIC, fsEnv)
    assign_fankuiJsonData.print("assign_fankuiJsonData =>")
    val FanKuiData: DataStream[Tbl_Assign_FeedBack] = assign_fankuiJsonData.process(new FanKuiBiaoDataFunction(modeldata))
    FanKuiData.addSink(fankuiSinkBuilder.build())


    //反馈明细表同步到ES
    val fkmxSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign_Fkmx](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign_Fkmx] {
        def process(tbl_assign_fkmx: Tbl_Assign_Fkmx, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil.getTblAssignFkmxJson(tbl_assign_fkmx)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_FKMX_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign_fkmx.fkid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到反馈明细表ES")
        }
      }
    )

    fkmxSinkBuilder.setBulkFlushMaxActions(5)
    fkmxSinkBuilder.setBulkFlushMaxSizeMb(5)
    fkmxSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    fkmxSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())

    //消费反馈明细表数据
    val assign_fkmxJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_FKMX_TOPIC, fsEnv)
    //assign_fkmxJsonData.print("assign_fkmxJsonData =>")

    val FkmxData: DataStream[Tbl_Assign_Fkmx] = assign_fkmxJsonData.process(new FkMingXiBiaoDataFunction(modeldata))
    FkmxData.addSink(fkmxSinkBuilder.build())


    //明细表同步到es
    val mxSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign_Mx](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign_Mx] {
        def process(tbl_assign_mx: Tbl_Assign_Mx, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil.getTblAssignMx(tbl_assign_mx)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_MX_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign_mx.pgmxid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到明细表ES")
        }
      }
    )

    mxSinkBuilder.setBulkFlushMaxActions(5)
    mxSinkBuilder.setBulkFlushMaxSizeMb(5)
    mxSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    mxSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())

    //消费明细表数据
    val assign_mxJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_MX_TOPIC, fsEnv)
    //assign_mxJsonData.print("assign_mxJsonData =>")

    val mxData: DataStream[Tbl_Assign_Mx] = assign_mxJsonData.process(new MingXiBiaoDataFunction(modeldata))
    mxData.addSink(mxSinkBuilder.build())


    //满意度表数据同步到ES
    val mydSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign_Satisfaction](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign_Satisfaction] {
        def process(tbl_assign_myd: Tbl_Assign_Satisfaction, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil.getTblAssignSatisfaction(tbl_assign_myd)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_SATISFACTION_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign_myd.id)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到满意度表ES")
        }
      }
    )

    mydSinkBuilder.setBulkFlushMaxActions(5)
    mydSinkBuilder.setBulkFlushMaxSizeMb(5)
    mydSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    mydSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())

    //消费满意度表数据
    val assign_mydJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_SATISFACTION_TOPIC, fsEnv)
    //assign_mydJsonData.print("assign_mydJsonData =>")

    val mydData: DataStream[Tbl_Assign_Satisfaction] = assign_mydJsonData.process(new ManYiBiaoDataFunction(modeldata))
    mydData.addSink(mydSinkBuilder.build())


    //新增阅读表数据同步到ES
    val xzydSinkBuilder = new ElasticsearchSink.Builder[Tbl_Assign_Xzyd](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Assign_Xzyd] {
        def process(tbl_assign_xzyd: Tbl_Assign_Xzyd, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val json = JsonBeanUtil.getTblAssignXzydJson(tbl_assign_xzyd)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_ASSIGN_XZYD_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(tbl_assign_xzyd.xzid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送数据到满意度表ES")
        }
      }
    )

    xzydSinkBuilder.setBulkFlushMaxActions(5)
    xzydSinkBuilder.setBulkFlushMaxSizeMb(5)
    xzydSinkBuilder.setBulkFlushInterval(1)
    // sink插入失败处理器
    xzydSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp())

    //消费新增阅读表数据
    val assign_xzydJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_ASSIGN_XZYD_TOPIC, fsEnv)
    //assign_xzydJsonData.print("assign_xzydJsonData =>")

    val xzydData: DataStream[Tbl_Assign_Xzyd] = assign_xzydJsonData.process(new XxxDataFunction(modeldata))
    xzydData.addSink(xzydSinkBuilder.build())

    //合流，将所有状态判断数据结果合并成一条流在判断是否存在es中
    val  ZhuangTaiPanDuan:DataStream[TrueData] = ZBData.getSideOutput(modeldata)
      .union(
        YuYueData.getSideOutput(modeldata),
        DaiJianData.getSideOutput(modeldata),
        FanKuiData.getSideOutput(modeldata),
        FkmxData.getSideOutput(modeldata),
        mxData.getSideOutput(modeldata),
        mydData.getSideOutput(modeldata),
        xzydData.getSideOutput(modeldata)
      ).process(new ZhuangTaiPanDuanFunction(zangshuju))

    //正确数据sink到业务域kafka
//    val TrueProduct = new FlinkKafkaProducer010[TrueData](
//      "WxData-T2", // sink  to topic
//      new UserKeyedSerializationSchema,
//      KafkaUtil.prop ,//sink到kafka的配置信息
//      new MyPartitioner
//    )
    ZhuangTaiPanDuan.addSink(KafkaUtil.getKfakaProducer("WxData-T2"))

    //将错误数据sink到脏数据使用的kafka中
//    val FalseProduct = new FlinkKafkaProducer010[TrueData](
//      "WxData-F2",
//      new UserKeyedSerializationSchema,
//      KafkaUtil.prop ,//sink到kafka的配置信息
//      new MyPartitioner
//    )
    ZhuangTaiPanDuan.getSideOutput(zangshuju).addSink(KafkaUtil.getKfakaProducer("WxData-F2"))


    fsEnv.execute("维修数据同步重构版")
  }

}
