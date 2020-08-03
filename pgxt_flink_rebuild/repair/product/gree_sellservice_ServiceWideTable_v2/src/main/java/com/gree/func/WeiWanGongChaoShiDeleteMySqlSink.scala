package com.gree.func

import com.gree.model.WxDataChaoShi
import com.gree.util.{JDBCUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class WeiWanGongChaoShiDeleteMySqlSink extends  RichSinkFunction[WxDataChaoShi]{
  val logger: Logger = LoggerFactory.getLogger(WeiWanGongChaoShiDeleteMySqlSink.super.getClass)

  override def invoke(value: WxDataChaoShi, context: SinkFunction.Context[_]): Unit = {
    val util = new NumberFormatUtil
    logger.info("逻辑判断工单已完工，从未完工Mysql数据删除...")
    //先删除
    try{
      logger.info("要删除的pgid为->"+value.pgid)
      JDBCUtil.executeUpdate("delete from wx_wait_timeout_orders1 where pgid=?", Array(util.panduanLong(value.pgid)))
    }catch {
      case e:Exception => logger.error("逻辑判断工单已完工，从未完工Mysql数据删除...MYSQL异常信息->"+e.getMessage)
    }
  }
}
