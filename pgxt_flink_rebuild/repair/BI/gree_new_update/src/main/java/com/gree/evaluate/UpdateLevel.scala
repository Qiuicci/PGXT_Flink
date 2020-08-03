package com.gree.evaluate

import com.gree.constant.Constant
import com.gree.evaluatesale.{UpdateWxBHKuanBiaoSale, UpdateWxBHMysqlKuanbiaoSale, UpdateWxDJMysqlKuanBiaoSale, UpdateWxDaiJianKuanBiaoSale, UpdateWxMaxKuanBiaoSale, UpdateWxMaxMysqlKuanbiaoSale, UpdateWxkbMaxKuanBiaoSale, UpdateWxkbTimeoutSale, UpdateWxkbWaitCompleteTimeoutSale}
import com.gree.model.{KuanBiaoDaiJianData, KuanBiaoData, MaxKuanBiao, WangdianLevel, WxDataChaoShi}
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}


object UpdateLevel {

  def main(args: Array[String]): Unit = {
    System.setProperty("io.netty.allocator.type", "unpooled")
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //数据输出到ES的连接
    val httpConnect = EsUtil.getEsHttpConnect
    val logger: Logger = LoggerFactory.getLogger(UpdateLevel.getClass)

    //任务并行度
    fsEnv.setParallelism(1)

    val wdLevelData=new OutputTag[WangdianLevel](Constant.WD_LEVEL_DATA)

    //消费TblWangdianSjdwmx表数据
    val wangdianSjdwmxJsonData = KafkaUtil.getKafkaConnect(Constant.TBL_WANGDIAN_SJDWMX_TOPIC, fsEnv)
    wangdianSjdwmxJsonData.print("wangdianSjdwmxJsonData->")

    //将消费的json数据解析成对应实体类，并比与之前比较判断
    val wdData: DataStream[WangdianLevel] = wangdianSjdwmxJsonData.process(new PanDuanLevelEqualsFunction(wdLevelData))


    //数据各自并行执行

    wdData.addSink(new UpdateWxkbWaitCompleteTimeout)
    val wxMaxKuanBiao: DataStream[KuanBiaoData] = wdData.process(new UpdateWxMaxKuanBiao)
    val wxBHKuanBIAO: DataStream[KuanBiaoData] = wdData.process(new UpdateWxBHKuanBiao)
    val wxDaiJianKuanBiao: DataStream[KuanBiaoDaiJianData] = wdData.process(new UpdateWxDaiJianKuanBiao)
    val wxMaxBigKuanBiao: DataStream[MaxKuanBiao] = wdData.process(new UpdateWxkbMaxKuanBiao)
    val wxWanGongChaoShi: DataStream[WxDataChaoShi] = wdData.process(new UpdateWxkbTimeout)
    wdData.addSink(new UpdateWxMaxMysqlKuanbiao)
    wdData.addSink(new UpdateWxBHMysqlKuanbiao)
    wdData.addSink(new UpdateWxDJMysqlKuanBiao)

    //销售数据
    val updateWxBHKuanBiaoSale: DataStream[KuanBiaoData] = wdData.getSideOutput[WangdianLevel](wdLevelData).process(new  UpdateWxBHKuanBiaoSale)
    wdData.getSideOutput[WangdianLevel](wdLevelData).addSink(new  UpdateWxBHMysqlKuanbiaoSale)
    val updateWxDaiJianKuanBiaoSale: DataStream[KuanBiaoDaiJianData] = wdData.getSideOutput[WangdianLevel](wdLevelData).process(new  UpdateWxDaiJianKuanBiaoSale)
    wdData.getSideOutput[WangdianLevel](wdLevelData).addSink(new  UpdateWxDJMysqlKuanBiaoSale)
    val updateWxkbMaxKuanBiaoSale: DataStream[MaxKuanBiao] = wdData.getSideOutput[WangdianLevel](wdLevelData).process(new  UpdateWxkbMaxKuanBiaoSale)
    wdData.getSideOutput[WangdianLevel](wdLevelData).addSink(new  UpdateWxkbWaitCompleteTimeoutSale)
    val updateWxMaxKuanBiaoSale: DataStream[KuanBiaoData] = wdData.getSideOutput[WangdianLevel](wdLevelData).process(new  UpdateWxMaxKuanBiaoSale)
    val updateWxWanGongChaoShi: DataStream[WxDataChaoShi] = wdData.getSideOutput[WangdianLevel](wdLevelData).process(new  UpdateWxkbTimeoutSale)
    wdData.getSideOutput[WangdianLevel](wdLevelData).addSink(new  UpdateWxMaxMysqlKuanbiaoSale)



    //输出到已完工超时表
    val chaoShiSink = new ElasticsearchSink.Builder[WxDataChaoShi](
      httpConnect,
      new ElasticsearchSinkFunction[WxDataChaoShi] {
        def process(wxDataChaoShi: WxDataChaoShi, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getWxkbTimeoutOrdersJson(wxDataChaoShi)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.WXKB_TIMEOUT_ORDER)
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
    wxWanGongChaoShi.addSink(chaoShiSink.build())
    updateWxWanGongChaoShi.addSink(chaoShiSink.build())


    //wx最大宽表Sink
    val kuanbiaoSink = new ElasticsearchSink.Builder[KuanBiaoData](
      httpConnect,
      new ElasticsearchSinkFunction[KuanBiaoData] {
        def process(maxKuanBiao:KuanBiaoData , ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getMaxKuanBiao(maxKuanBiao)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.MAX_KUAN_BIAO)
            .`type`(Constant.ES_TYPE)
            .id(maxKuanBiao.pgid)
            .source(json)

          indexer.add(rqst)
          logger.info("gree_new_update发送到最大宽表es")
        }
      }
    )
    kuanbiaoSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    kuanbiaoSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    kuanbiaoSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    wxMaxKuanBiao.addSink(kuanbiaoSink.build())
    updateWxMaxKuanBiaoSale.addSink(kuanbiaoSink.build())

    //wx驳回宽表Sink
    val bohuiKuanBiaoSink = new ElasticsearchSink.Builder[KuanBiaoData](
      httpConnect,
      new ElasticsearchSinkFunction[KuanBiaoData] {
        def process(boHuiKuanBiao:KuanBiaoData , ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getBoHuiKuanBiao(boHuiKuanBiao)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.BO_HUI_KUAN_BIAO)
            .`type`(Constant.ES_TYPE)
            .id(boHuiKuanBiao.pgid)
            .source(json)

          indexer.add(rqst)
          logger.info("gree_new_update发送到驳回宽表es")
        }
      }
    )
    bohuiKuanBiaoSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    bohuiKuanBiaoSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    bohuiKuanBiaoSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    wxBHKuanBIAO.addSink(bohuiKuanBiaoSink.build())
    updateWxBHKuanBiaoSale.addSink(bohuiKuanBiaoSink.build())

    //wx待件宽表Sink
    val daiJianKuanBiaoSink = new ElasticsearchSink.Builder[KuanBiaoDaiJianData](
      httpConnect,
      new ElasticsearchSinkFunction[KuanBiaoDaiJianData] {
        def process(daijianKuanBiao:KuanBiaoDaiJianData , ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getDaiJianKuanBiao(daijianKuanBiao)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.DAI_JIAN_KUAN_BIAO)
            .`type`(Constant.ES_TYPE)
            .id(daijianKuanBiao.assignDaiJianid)
            .source(json)
          indexer.add(rqst)
          logger.info("gree_new_update发送到待件宽表es")
        }
      }
    )
    daiJianKuanBiaoSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    daiJianKuanBiaoSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    daiJianKuanBiaoSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    wxDaiJianKuanBiao.addSink(daiJianKuanBiaoSink.build())
    updateWxDaiJianKuanBiaoSale.addSink(daiJianKuanBiaoSink.build())

    //wx最大宽表Sink
    //大宽表sink
    val maxKuanBiaoSink = new ElasticsearchSink.Builder[MaxKuanBiao](
      httpConnect,
      new ElasticsearchSinkFunction[MaxKuanBiao] {
        def process(maxKuanBiao: MaxKuanBiao, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = JsonBeanUtil.getWxkbMaxBigKuanBiaoJson(maxKuanBiao)
          val rqst: IndexRequest = Requests.indexRequest
            .index(Constant.WXKB_MAX_BIG_KUANBIAO)
            .`type`(Constant.ES_TYPE)
            .id(maxKuanBiao.pgid)
            .source(json)

          indexer.add(rqst)

          logger.info("发送到超大宽表")
        }
      }
    )
    maxKuanBiaoSink.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
    maxKuanBiaoSink.setBulkFlushMaxSizeMb(Constant.FIVE)
    maxKuanBiaoSink.setBulkFlushInterval(Constant.TWO_THOUSAND)

    wxMaxBigKuanBiao.addSink(maxKuanBiaoSink.build())
    updateWxkbMaxKuanBiaoSale.addSink(maxKuanBiaoSink.build())

    fsEnv.execute("维修批量更新权限层级")
  }
}