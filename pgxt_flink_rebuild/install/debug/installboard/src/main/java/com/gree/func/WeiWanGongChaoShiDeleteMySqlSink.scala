package com.gree.func

import com.gree.model.AzDataChaoShi
import com.gree.util.JDBCUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class WeiWanGongChaoShiDeleteMySqlSink  extends RichSinkFunction[AzDataChaoShi]{
  val logger: Logger = LoggerFactory.getLogger(WeiWanGongChaoShiDeleteMySqlSink.super.getClass)
  override def invoke(value: AzDataChaoShi, context: SinkFunction.Context[_]): Unit = {

    logger.info("逻辑判断工单已完工，从未完工Mysql数据删除...")
    //先删除
    try{
      logger.info("安装要删除的pgguid为->"+value.pgguid)
      JDBCUtil.executeUpdate("delete from az_wait_timeout_orders1 where pgguid=?", Array(value.pgguid))
    }catch {
      case e:Exception => logger.error("逻辑判断工单已完工，从未完工Mysql数据删除...MYSQL异常信息->"+e.getMessage)
    }
  }
}
