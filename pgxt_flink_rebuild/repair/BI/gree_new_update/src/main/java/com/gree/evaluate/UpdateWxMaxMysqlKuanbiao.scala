package com.gree.evaluate

import com.gree.model.WangdianLevel
import com.gree.util.JDBCUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class UpdateWxMaxMysqlKuanbiao extends RichSinkFunction[WangdianLevel]{
  val logger: Logger = LoggerFactory.getLogger(UpdateWxMaxMysqlKuanbiao.super.getClass)

  override def invoke(value: WangdianLevel, context: SinkFunction.Context[_]): Unit = {
    JDBCUtil.executeUpdate(
      "update wx_all_orders1 set level1 = ? ,level2 =? ,level3 =? ,level4 =? ,level5 =? ,level6 =? ,level7 =? where spid= ? and xjwdno = ?",
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
