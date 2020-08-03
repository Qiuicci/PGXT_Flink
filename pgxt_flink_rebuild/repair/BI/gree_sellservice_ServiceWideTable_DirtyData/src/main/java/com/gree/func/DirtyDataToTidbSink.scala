package com.gree.func

import com.gree.model.DirtyData
import com.gree.util.{JDBCUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class DirtyDataToTidbSink  extends  RichSinkFunction[DirtyData]{
  val logger: Logger = LoggerFactory.getLogger(DirtyDataToTidbSink.super.getClass)

  override def invoke(value: DirtyData, context: SinkFunction.Context[_]): Unit = {
    this.synchronized{
      val util = new NumberFormatUtil
      try{
        JDBCUtil.executeUpdate("insert into service_dirtydata values(?,?,?,?,?,?)",Array(value.id,value.pgid,value.table,value.ts,value.state,value.describe))
      }catch{
        case  e:Exception => logger.error("插入tidb错误"+e.getMessage)
      }
    }

  }
}
