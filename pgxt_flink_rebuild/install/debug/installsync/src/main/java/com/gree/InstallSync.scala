package com.gree

import java.util.concurrent.TimeUnit

import com.gree.async.AsyncFromES
import com.gree.constant.Constant
import com.gree.func.{Az_Assign_Appointment_ES, Az_Assign_Fee_ES, Az_Assign_Fkmx_ES, Az_Assign_Lc_Fzry_ES, Az_Assign_Lc_Ls_ES, Az_Assign_Mx_ES, Az_Assign_Platform_ES, Az_Assign_Pslc_Ls_ES, Az_Assign_Satisfaction_ES, Check_Exist_Func, Query_From_ES, Trade_New_For_Old_ES, Yjhx_Jdd_ES}
import com.gree.model.{AzKafkaBuidData, Tbl_Az_Assign_Appointment, Tbl_Az_Assign_Fee, Tbl_Az_Assign_Fkmx, Tbl_Az_Assign_Lc_Fzry, Tbl_Az_Assign_Lc_Ls, Tbl_Az_Assign_Mx, Tbl_Az_Assign_Platform, Tbl_Az_Assign_Pslc_Ls, Tbl_Az_Assign_Satisfaction, Tbl_Trade_New_For_Old, Tbl_Yjhx_Jdd}
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}

object InstallSync {
  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect
    val logger: Logger = LoggerFactory.getLogger(InstallSync.getClass)

    //任务并行度
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val noExistRecord = new OutputTag[AzKafkaBuidData]("noExistRecord")
    val tblAzAssignLcLs = new OutputTag[Tbl_Az_Assign_Lc_Ls](Constant.Tbl_Az_Assign_Lc_Ls)
    val tblAzAssignAppointment = new OutputTag[Tbl_Az_Assign_Appointment](Constant.Tbl_Az_Assign_Appointment)
    val tblAzAssignFkmx = new OutputTag[Tbl_Az_Assign_Fkmx](Constant.Tbl_Az_Assign_Fkmx)
    val tblAzAssignMx = new OutputTag[Tbl_Az_Assign_Mx](Constant.Tbl_Az_Assign_Mx)
    val tblAzAssignSatisfaction = new OutputTag[Tbl_Az_Assign_Satisfaction](Constant.Tbl_Az_Assign_Satisfaction)
    val tblAzAssignPslcLs = new OutputTag[Tbl_Az_Assign_Pslc_Ls](Constant.Tbl_Az_Assign_Pslc_Ls)
    val tblAzAssignFee = new OutputTag[Tbl_Az_Assign_Fee](Constant.TBL_AZ_ASSIGN_FEE)
    val tblAzAssignLcFzry = new OutputTag[Tbl_Az_Assign_Lc_Fzry](Constant.TBL_AZ_ASSIGN_LC_FZRY)
    val tblAzAssignPlatform = new OutputTag[Tbl_Az_Assign_Platform](Constant.TBL_AZ_ASSIGN_PLATFORM)
    val tblTradeNewForOld = new OutputTag[Tbl_Trade_New_For_Old](Constant.TBL_TRADE_NEW_FOR_OLD)
    val tblYjhxJdd = new OutputTag[Tbl_Yjhx_Jdd](Constant.TBL_YJHX_JDD)

    /**
      * 消费Tbl_Az_Assign_Lc_Ls
      */
    val az_Assign_Lc_Ls_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_TOPIC, fsEnv)
    //az_Assign_Lc_Ls_Kafka.print("az_Assign_Lc_Ls_Kafka Data->")
    val tblAzAssignLcLsData: DataStream[Tbl_Az_Assign_Lc_Ls] = az_Assign_Lc_Ls_Kafka.process(new Az_Assign_Lc_Ls_ES(tblAzAssignLcLs))

    /**
      * Tbl_Az_Assign_Lc_Ls的sink到es的builder
      */
    val tblAzAssignLcLsESSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Lc_Ls](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Lc_Ls] {
        def process(assign_Lc_Ls: Tbl_Az_Assign_Lc_Ls, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignLcLsJson(assign_Lc_Ls)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_ASSIGN_LC_LS)
            .`type`(Constant.ES_TYPE)
            .id(assign_Lc_Ls.pgguid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到Tbl_Az_Assign_Lc_Lses")
        }
      }
    )
    tblAzAssignLcLsESSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    tblAzAssignLcLsESSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    tblAzAssignLcLsESSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    /**
      * sink的Tbl_Az_Assign_Lc_Ls到es
      */
    tblAzAssignLcLsData.addSink(tblAzAssignLcLsESSink.build())

    /**
      * 预约表
      * 消费Tbl_Az_Assign_Appointment
      */
    val az_Assign_Appointment_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_APPOINTMENT_TOPIC, fsEnv)
    //az_Assign_Appointment_Kafka.print("az_Assign_Appointment_Kafka Data->")
    val azAssignAppointmentData: DataStream[Tbl_Az_Assign_Appointment] = az_Assign_Appointment_Kafka.process(new Az_Assign_Appointment_ES(tblAzAssignAppointment))

    /**
      * Tbl_Az_Assign_Appointment的sink到es的builder
      */
    val azAssignAppointmentESSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Appointment](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Appointment] {
        def process(appointment: Tbl_Az_Assign_Appointment, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignAppointmentJson(appointment)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_ASSIGN_APPOINTMENT)
            .`type`(Constant.ES_TYPE)
            .id(appointment.id)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到Tbl_Az_Assign_Appointment表es")
        }
      }
    )
    azAssignAppointmentESSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azAssignAppointmentESSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azAssignAppointmentESSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**
      * sink的Tbl_Az_Assign_Appointment到es
      */
    azAssignAppointmentData.addSink(azAssignAppointmentESSink.build())

    /**
      * 反馈明细
      * 消费Tbl_Az_Assign_Fkmx
      */
    val az_Assign_Fkmx_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_FKMX_TOPIC, fsEnv)
    //az_Assign_Fkmx_Kafka.print("az_Assign_Fkmx_Kafka->")
    val azAssignFkmxData: DataStream[Tbl_Az_Assign_Fkmx] = az_Assign_Fkmx_Kafka.process(new Az_Assign_Fkmx_ES(tblAzAssignFkmx))

    /**
      * Tbl_Az_Assign_Fkmx的sink到es的builder
      */
    val azAssignFkmxESSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Fkmx](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Fkmx] {
        def process(fkmx: Tbl_Az_Assign_Fkmx, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignFkmxJson(fkmx)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_ASSIGN_FKMX)
            .`type`(Constant.ES_TYPE)
            .id(fkmx.fkid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到Tbl_Az_Assign_Fkmx表es")
        }
      }
    )
    azAssignFkmxESSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azAssignFkmxESSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azAssignFkmxESSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**
      * sink的Tbl_Az_Assign_Fkmx到es
      */
    azAssignFkmxData.addSink(azAssignFkmxESSink.build())

    /**
      *安装满意度表
      * 消费Tbl_Az_Assign_Satisfaction
      */
    val az_Assign_Satisfaction_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_SATISFACTION_TOPIC, fsEnv)
    //az_Assign_Satisfaction_Kafka.print("az_Assign_Satisfaction_Kafka Data->")
    val azAssignSatisfactionData: DataStream[Tbl_Az_Assign_Satisfaction] = az_Assign_Satisfaction_Kafka.process(new Az_Assign_Satisfaction_ES(tblAzAssignSatisfaction))

    /**
      * Tbl_Az_Assign_Satisfaction的sink到es的builder
      */
    val azAssignSatisfactionESSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Satisfaction](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Satisfaction] {
        def process(satisfaction: Tbl_Az_Assign_Satisfaction, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignSatisfaction(satisfaction)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_SATISFACTION)
            .`type`(Constant.ES_TYPE)
            .id(satisfaction.id)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到Tbl_Az_Assign_Satisfaction表es")
        }
      }
    )
    azAssignSatisfactionESSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azAssignSatisfactionESSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azAssignSatisfactionESSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**
      * sink的Tbl_Az_Assign_Satisfaction到es
      */
    azAssignSatisfactionData.addSink(azAssignSatisfactionESSink.build())

    /**
      * 安装明细表
      * 消费Tbl_Az_Assign_Mx
      */
    val az_Assign_Mx_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_MX_TOPIC, fsEnv)
    //az_Assign_Mx_Kafka.print("az_Assign_Mx_Kafka Data->")
    val azAssignMxData: DataStream[Tbl_Az_Assign_Mx] = az_Assign_Mx_Kafka.process(new Az_Assign_Mx_ES(tblAzAssignMx))

    /**
      * Tbl_Az_Assign_Mx的sink到es的builder
      */
    val azAssignMxESSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Mx](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Mx] {
        def process(mx: Tbl_Az_Assign_Mx, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignMx(mx)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_MX)
            .`type`(Constant.ES_TYPE)
            .id(mx.pgmxid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到Tbl_Az_Assign_Mx表es")
        }
      }
    )
    azAssignMxESSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azAssignMxESSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azAssignMxESSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**
      * sink的Tbl_Az_Assign_Mx到es
      */
    azAssignMxData.addSink(azAssignMxESSink.build())

    /**
      * 配送表
      * 消费Tbl_Az_Assign_Pslc_Ls
      */
    val az_Assign_Pslc_Ls_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_PSLC_LS_TOPIC, fsEnv)
    //az_Assign_Pslc_Ls_Kafka.print("Tbl_Az_Assign_Pslc_Ls Data->")
    val azAssignPslcLsData: DataStream[Tbl_Az_Assign_Pslc_Ls] = az_Assign_Pslc_Ls_Kafka.process(new Az_Assign_Pslc_Ls_ES(tblAzAssignPslcLs))

    /**
      * Tbl_Az_Assign_Pslc_Ls的sink到es的builder
      */
    val azAssignPslcLsESSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Pslc_Ls](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Pslc_Ls] {
        def process(pslc: Tbl_Az_Assign_Pslc_Ls, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignPslclsJson(pslc)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_ASSIGN_PSLC_LS)
            .`type`(Constant.ES_TYPE)
            .id(pslc.pgguid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到Tbl_Az_Assign_Pslc_Ls表es")
        }
      }
    )
    azAssignPslcLsESSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azAssignPslcLsESSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azAssignPslcLsESSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**
      * sink的Tbl_Az_Assign_Pslc_Ls到es
      */
    azAssignPslcLsData.addSink(azAssignPslcLsESSink.build())


    /**
      *  费用表
      * 消费Tbl_Az_Assign_Fee
      */
    val az_Assign_Fee_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_FEE_TOPIC, fsEnv)
    //az_Assign_Fee_Kafka.print("Tbl_Az_Assign_Fee Data->")
    val azAssignFeeData: DataStream[Tbl_Az_Assign_Fee] = az_Assign_Fee_Kafka.process(new Az_Assign_Fee_ES(tblAzAssignFee))

    /**
      * Tbl_Az_Assign_Fee的sink到es的builder
      */
    val  azAssignFeeESSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Fee](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Fee] {
        def process(assignFree: Tbl_Az_Assign_Fee, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignFeeJson(assignFree)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_ASSIGN_FEE)
            .`type`(Constant.ES_TYPE)
            .id(assignFree.id)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到Tbl_Az_Assign_Fee_Ls表es")
        }
      }
    )
    azAssignFeeESSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azAssignFeeESSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azAssignFeeESSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**
      * sink的Tbl_Az_Assign_Fee到es
      */
    azAssignFeeData.addSink(azAssignFeeESSink.build())


    /**
      *  费用表
      * 消费Tbl_Az_Assign_Lc_Fzry
      */
    val az_Assign_Lc_Fzry_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_LC_FZRY_TOPIC, fsEnv)
    //az_Assign_Lc_Fzry_Kafka.print("Tbl_Az_Assign_Lc_Fzry Data->")
    val azAssignLcFzryData: DataStream[Tbl_Az_Assign_Lc_Fzry] = az_Assign_Lc_Fzry_Kafka.process(new Az_Assign_Lc_Fzry_ES(tblAzAssignLcFzry))

    /**
      * Tbl_Az_Assign_Lc_Fzry的sink到es的builder
      */
    val assignLcFzrySink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Lc_Fzry](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Lc_Fzry] {
        def process(tblAzAssignLcFzry: Tbl_Az_Assign_Lc_Fzry, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignLcFzryJson(tblAzAssignLcFzry)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_ASSIGN_LC_FZRY)
            .`type`(Constant.ES_TYPE)
            .id(tblAzAssignLcFzry.id)
            .source(json)
          indexer.add(rqst)

          logger.info("发送default_server_greeshinstall_tbl_az_assign_lc_fzry_v1到es")
        }
      }
    )
    assignLcFzrySink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    assignLcFzrySink.setBulkFlushMaxSizeMb(Constant.FIVE)
    assignLcFzrySink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**
      * sink的Tbl_Az_Assign_Lc_Fzry
      */
    azAssignLcFzryData.addSink(assignLcFzrySink.build())


    /**
      *  费用表
      * 消费Tbl_Az_Assign_Platform
      */
    val az_Assign_Platform_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_AZ_ASSIGN_PLATFORM_TOPIC, fsEnv)
    //az_Assign_Platform_Kafka.print("Tbl_Az_Assign_Platform Data->")
    val azAssignPlatformData: DataStream[Tbl_Az_Assign_Platform] = az_Assign_Platform_Kafka.process(new Az_Assign_Platform_ES(tblAzAssignPlatform))

    /**
      * Tbl_Az_Assign_Platform的sink到es的builder
      */

    val assignPlatformSink = new ElasticsearchSink.Builder[Tbl_Az_Assign_Platform](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Az_Assign_Platform] {
        def process(assignPlatform: Tbl_Az_Assign_Platform, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzAssignPlatformJson(assignPlatform)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_ASSIGN_PLATFORM)
            .`type`(Constant.ES_TYPE)
            .id(assignPlatform.id)
            .source(json)
          indexer.add(rqst)

          logger.info("发送default_server_greeshinstall_tbl_az_assign_platform_v1到es")
        }
      }
    )
    assignPlatformSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    assignPlatformSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    assignPlatformSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    /**
      * sink的Tbl_Az_Assign_Platform
      */
    azAssignPlatformData.addSink(assignPlatformSink.build())


    /**
      *  费用表
      * 消费Tbl_Trade_New_For_Old
      */
    val trade_New_For_Old_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_TRADE_NEW_FOR_OLD_TOPIC, fsEnv)
    //trade_New_For_Old_Kafka.print("Tbl_Trade_New_For_Old Data->")
    val tradeNewForOldData: DataStream[Tbl_Trade_New_For_Old] = trade_New_For_Old_Kafka.process(new Trade_New_For_Old_ES(tblTradeNewForOld))

    /**
      * Tbl_Trade_New_For_Old的sink到es的builder
      */

    val tradeNewForOldSink = new ElasticsearchSink.Builder[Tbl_Trade_New_For_Old](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Trade_New_For_Old] {
        def process(tblTradeNewForOld: Tbl_Trade_New_For_Old, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblAzTradeNewForOldJson(tblTradeNewForOld)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_TRADE_NEW_FOR_OLD)
            .`type`(Constant.ES_TYPE)
            .id(tblTradeNewForOld.id)
            .source(json)
          indexer.add(rqst)

          logger.info("发送default_server_greeshinstall_tbl_trade_new_for_old_V1到es")
        }
      }
    )
    tradeNewForOldSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    tradeNewForOldSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    tradeNewForOldSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    /**
      * sink的Tbl_Trade_New_For_Old
      */
    tradeNewForOldData.addSink(tradeNewForOldSink.build())


    /**
      *  费用表
      * 消费Tbl_Trade_New_For_Old
      */
    val yjhx_Jdd_Kafka = KafkaUtil.getKafkaConnect(Constant.TBL_YJHX_JDD_TOPIC, fsEnv)
    //yjhx_Jdd_Kafka.print("Tbl_Yjhx_Jdd Data->")
    val yjhxJddData: DataStream[Tbl_Yjhx_Jdd] = yjhx_Jdd_Kafka.process(new Yjhx_Jdd_ES(tblYjhxJdd))

    /**
      *  Tbl_Yjhx_Jdd的sink到es的builder
      */

    val yjhxJddSink = new ElasticsearchSink.Builder[Tbl_Yjhx_Jdd](
      httpConnect,
      new ElasticsearchSinkFunction[Tbl_Yjhx_Jdd] {
        def process(tblYjhxJdd: Tbl_Yjhx_Jdd, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getTblYjhxJddJson(tblYjhxJdd)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_YJHX_JDD)
            .`type`(Constant.ES_TYPE)
            .id(tblYjhxJdd.id)
            .source(json)
          indexer.add(rqst)

          logger.info("发送default_server_greeshinstall_tbl_yjhx_jdd_V1到es")
        }
      }
    )
    yjhxJddSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    yjhxJddSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    yjhxJddSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    /**Tbl_Yjhx_JddTbl_Trade_New_For_Old
      */
    yjhxJddData.addSink(yjhxJddSink.build())



    var checkazdata:DataStream[(String,String,String,String,String,String)] = tblAzAssignLcLsData.getSideOutput(tblAzAssignLcLs).map(a => (Constant.TBL_AZ_ASSIGN_LC_LS, "pgguid", a.pgguid, a.pgguid, a.ts ,a.table)) //查询是否已经在es已经存在数据,tblAzAssignLcLsTopic
      .union(azAssignAppointmentData.getSideOutput(tblAzAssignAppointment).map(a => (Constant.TBL_AZ_ASSIGN_APPOINTMENT, "id", a.id, a.pgguid, a.ts ,a.table))
      ,azAssignFkmxData.getSideOutput(tblAzAssignFkmx).map(a => (Constant.TBL_AZ_ASSIGN_FKMX, "fkid", a.fkid, a.pgguid, a.ts ,a.table))
      ,azAssignSatisfactionData.getSideOutput(tblAzAssignSatisfaction).map(a => (Constant.TBL_AZ_SATISFACTION, "id", a.id, a.pgguid, a.ts ,a.table))
      ,azAssignMxData.getSideOutput(tblAzAssignMx).map(a => (Constant.TBL_AZ_MX, "pgmxid", a.pgmxid, a.pgguid, a.ts ,a.table))
      ,azAssignPslcLsData.getSideOutput(tblAzAssignPslcLs).map(a => (Constant.TBL_AZ_ASSIGN_PSLC_LS, "pgguid", a.pgguid, a.pgguid, a.ts ,a.table))
      ,azAssignFeeData.getSideOutput(tblAzAssignFee).map(a => (Constant.TBL_AZ_ASSIGN_FEE, "id", a.id, a.pgguid, a.ts ,a.table))
      ,azAssignPlatformData.getSideOutput(tblAzAssignPlatform).map(a => (Constant.TBL_AZ_ASSIGN_PLATFORM, "id", a.id, a.pgguid, a.ts ,a.table))
      ,tradeNewForOldData.getSideOutput(tblTradeNewForOld).map(a => (Constant.TBL_TRADE_NEW_FOR_OLD, "id", a.id, a.pgguid, a.ts ,a.table))
      ,yjhxJddData.getSideOutput(tblYjhxJdd).map(a => (Constant.TBL_YJHX_JDD, "id", a.id, a.pgguid, a.ts ,a.table))
        ,azAssignLcFzryData.getSideOutput(tblAzAssignLcFzry).map( a =>(Constant.TBL_AZ_ASSIGN_LC_FZRY, "id", a.id, a.pgguid, a.ts ,a.table))
    )

/*
var checkazdata:DataStream[(String,String,String,String,String,String)] = azAssignFkmxData.getSideOutput(tblAzAssignFkmx).map(a => (Constant.TBL_AZ_ASSIGN_FKMX, "fkid", a.fkid, a.pgguid, a.ts ,a.table))
  .union(tblAzAssignLcLsData.getSideOutput(tblAzAssignLcLs).map(a => (Constant.TBL_AZ_ASSIGN_LC_LS, "pgguid", a.pgguid, a.pgguid, a.ts ,a.table)))*/
    //查询是否已经在es已经存在数据,tblAzAssignLcLsTopic
    val tblAzAssignLcLsTopic = AsyncDataStream.orderedWait(checkazdata
      ,new AsyncFromES(),500L,TimeUnit.MILLISECONDS,100).process(new Check_Exist_Func(noExistRecord)).name("返查ES是否有数据")

     //val tblAzAssignLcLsTopic = checkazdata.process(new Query_From_ES).process(new Check_Exist_Func(noExistRecord)).name("返查ES是否有数据")

    tblAzAssignLcLsTopic.addSink(KafkaUtil.getKafkaProducer("AzData-goodRecord")).name("推送到正常数据流")
    tblAzAssignLcLsTopic.getSideOutput(noExistRecord).addSink(KafkaUtil.getKafkaProducer("AzData-badRecord")).name("推送到脏数据流")

    fsEnv.execute("安装同步重构版本1.0")


  }
}
