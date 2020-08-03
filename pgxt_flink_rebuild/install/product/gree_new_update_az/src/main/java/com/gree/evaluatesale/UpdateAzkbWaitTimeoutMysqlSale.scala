package com.gree.evaluatesale

import com.gree.model.WangdianLevel
import com.gree.util.JDBCUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class UpdateAzkbWaitTimeoutMysqlSale extends RichSinkFunction[WangdianLevel]{
  val logger: Logger = LoggerFactory.getLogger(UpdateAzkbWaitTimeoutMysqlSale.super.getClass)

  override def invoke(value: WangdianLevel, context: SinkFunction.Context[_]): Unit = {
    JDBCUtil.executeUpdate(
      "update az_wait_timeout_orders1 set level8 = ? ,level9 =? ,level10 =? ,level11 =? ,level12 =? ,level13 =? ,level14 =? where spid= ? and cjwdno = ?",
      Array(value.first,
        value.second,
        value.third,
        value.fourth,
        value.fifth,
        value.sixth,
        value.seventh,
        value.splb,
        value.wdno
      ))
  }

}
