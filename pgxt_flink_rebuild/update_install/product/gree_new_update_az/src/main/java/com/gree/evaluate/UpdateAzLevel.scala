package com.gree.evaluate

import com.gree.constant.Constant
import com.gree.evaluatesale.{UpdateAzkbMaxBigKuanBiaoSale, UpdateAzkbTimeOutOrderSale, UpdateAzkbWaitTimeoutMysqlSale, UpdateAzkuanbiaoAllOrderSale, UpdateAzkuanbiaoCaiwuSale, UpdateAzkuanbiaoChanPinMingXiSale}
import com.gree.model.{AZMaxOrder, AzCaiWuDaoChuData, AzChanPinMingXiKuanBiaoData, AzDataChaoShi, MaxKuanBiaoData, WangdianLevel}
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}


object UpdateAzLevel {

  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect
    val logger: Logger = LoggerFactory.getLogger(UpdateAzLevel.getClass)

    //任务并行度
    fsEnv.setParallelism(3)

    val wdLevelData=new OutputTag[WangdianLevel](Constant.WD_LEVEL_DATA)

    //消费TblWangdianSjdwmx表数据
    val wangdianSjdwmxJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_WANGDIAN_SJDWMX_TOPIC, fsEnv)
    wangdianSjdwmxJsonData.print("wangdianSjdwmxJsonData->")

    //将消费的json数据解析成对应实体类，并比与之前比较判断
    val wdData: DataStream[WangdianLevel] = wangdianSjdwmxJsonData.process(new PanDuanLevelEqualsFunction(wdLevelData))


    //azkb_timeout_orders_v1
    val AzkbTimeOutOrder: DataStream[AzDataChaoShi] = wdData.process(new UpdateAzkbTimeOutOrder)
    val AzkbTimeOutOrderSink = new ElasticsearchSink.Builder[AzDataChaoShi](
      httpConnect,
      new ElasticsearchSinkFunction[AzDataChaoShi] {
        def process(azDataChaoShi: AzDataChaoShi, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getAzkbTimeoutOrdersJson(azDataChaoShi)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.AZKB_TIMEOUT_ORDER)//azkb_timeout_orders_v3
            .`type`(Constant.ES_TYPE)
            .id(azDataChaoShi.pgguid)
            .source(json)
          indexer.add(rqst)
          logger.info("gree_new_AZupdate发送到azkb_timeout_orders_v3")
        }
      }
    )
    AzkbTimeOutOrderSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    AzkbTimeOutOrderSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    AzkbTimeOutOrderSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    AzkbTimeOutOrder.addSink(AzkbTimeOutOrderSink.build())
    wdData.getSideOutput[WangdianLevel](wdLevelData).process(new UpdateAzkbTimeOutOrderSale).addSink(AzkbTimeOutOrderSink.build())

    //更新mysql 的数据
    wdData.addSink(new UpdateAzkbWaitTimeoutMysql)
    wdData.getSideOutput[WangdianLevel](wdLevelData).addSink(new UpdateAzkbWaitTimeoutMysqlSale)

    /////////////////更新宽表/////////////////////////////////////////////
    //az_export_data_product_orders_v1
    val AzkuanbiaoChanPinMingXi: DataStream[AzChanPinMingXiKuanBiaoData] = wdData.process(new UpdateAzkuanbiaoChanPinMingXi)
    val AzkuanbiaoChanPinMingXiSink = new ElasticsearchSink.Builder[AzChanPinMingXiKuanBiaoData](
      httpConnect,
      new ElasticsearchSinkFunction[AzChanPinMingXiKuanBiaoData] {
        def process(azChanPinMingXiKuanBiaoData: AzChanPinMingXiKuanBiaoData, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getAzCanPinMingXiTable(azChanPinMingXiKuanBiaoData)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_CHAN_PIN_MING_XI_INDEX)//"az_export_data_product_orders_v3"
            .`type`(Constant.ES_TYPE)
            .id(azChanPinMingXiKuanBiaoData.pgguid)
            .source(json)
          indexer.add(rqst)
          logger.info("gree_new_AZupdate发送到az_export_data_product_orders_v3")
        }
      }
    )
    AzkuanbiaoChanPinMingXiSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    AzkuanbiaoChanPinMingXiSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    AzkuanbiaoChanPinMingXiSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    AzkuanbiaoChanPinMingXi.addSink(AzkuanbiaoChanPinMingXiSink.build())
    wdData.getSideOutput[WangdianLevel](wdLevelData).process(new UpdateAzkuanbiaoChanPinMingXiSale).addSink(AzkuanbiaoChanPinMingXiSink.build())

    //az_export_data_all_orders_v1
    val AzkuanbiaoAllOrder: DataStream[MaxKuanBiaoData] = wdData.process(new UpdateAzkuanbiaoAllOrder)
    val AzkuanbiaoAllOrderSink = new ElasticsearchSink.Builder[MaxKuanBiaoData](
      httpConnect,
      new ElasticsearchSinkFunction[MaxKuanBiaoData] {
        def process(maxKuanBiaoData: MaxKuanBiaoData, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getMaxAnZhuangTable(maxKuanBiaoData)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_EXPORT_WIDEN_INDEX)
            .`type`(Constant.ES_TYPE)
            .id(maxKuanBiaoData.pgguid)
            .source(json)
          indexer.add(rqst)
          logger.info("gree_new_AZupdate发送到azkb_timeout_orders_v3")
        }
      }
    )
    AzkuanbiaoAllOrderSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    AzkuanbiaoAllOrderSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    AzkuanbiaoAllOrderSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    AzkuanbiaoAllOrder.addSink(AzkuanbiaoAllOrderSink.build())
    wdData.getSideOutput[WangdianLevel](wdLevelData).process(new UpdateAzkuanbiaoAllOrderSale).addSink(AzkuanbiaoAllOrderSink.build())

    //az_export_data_finance_orders_v1
    val AzkuanbiaoCaiwu: DataStream[AzCaiWuDaoChuData] = wdData.process(new UpdateAzkuanbiaoCaiwu)
    val AzkuanbiaoCaiwuSink = new ElasticsearchSink.Builder[AzCaiWuDaoChuData](
      httpConnect,
      new ElasticsearchSinkFunction[AzCaiWuDaoChuData] {
        def process(azCaiWuDaoChuData: AzCaiWuDaoChuData, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getCaiWuExportWidenTable(azCaiWuDaoChuData)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.TBL_AZ_FINANCE_WIDEN_INDEX)//"az_export_data_finance_orders_v3"
            .`type`(Constant.ES_TYPE)
            .id(azCaiWuDaoChuData.tblAzAssignMxPgmxid)
            .source(json)
          indexer.add(rqst)
          logger.info("gree_new_AZupdate发送到az_export_data_finance_orders_v3")
        }
      }
    )
    AzkuanbiaoCaiwuSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    AzkuanbiaoCaiwuSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    AzkuanbiaoCaiwuSink.setBulkFlushInterval(Constant.TWO_THOUSAND)
    AzkuanbiaoCaiwu.addSink(AzkuanbiaoCaiwuSink.build())
    wdData.getSideOutput[WangdianLevel](wdLevelData).process(new UpdateAzkuanbiaoCaiwuSale).addSink(AzkuanbiaoCaiwuSink.build())


    val maxBigKuanBiao: DataStream[AZMaxOrder] = wdData.process(new UpdateAzkbMaxBigKuanBiao)
    //数据直接输AZmaxorder的Sink
    val azMaxKuanBiaoSink = new ElasticsearchSink.Builder[AZMaxOrder](
      httpConnect,
      new ElasticsearchSinkFunction[AZMaxOrder] {
        def process(azmaxOrder: AZMaxOrder, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getAzkbMaxBigKuanBiaoJson(azmaxOrder)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.WXKB_MAX_BIG_KUANBIAO)
            .`type`(Constant.ES_TYPE)
            .id(azmaxOrder.pgguid)
            .source(json)
          indexer.add(rqst)
          logger.info("发送到安装最大宽表es")
        }
      }
    )
    azMaxKuanBiaoSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    azMaxKuanBiaoSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    azMaxKuanBiaoSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    maxBigKuanBiao.addSink(azMaxKuanBiaoSink.build())
    wdData.getSideOutput[WangdianLevel](wdLevelData).process(new UpdateAzkbMaxBigKuanBiaoSale).addSink(azMaxKuanBiaoSink.build())



    fsEnv.execute("安装批量更新权限层级")


  }
}
