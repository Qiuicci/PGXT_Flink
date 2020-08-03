package com.gree.func

import com.gree.model.AzKafkaBuidData
import com.gree.util.{JDBCUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class Bad_Record_Into_Mysql extends RichSinkFunction[(AzKafkaBuidData,Int,String)]{
  val logger: Logger = LoggerFactory.getLogger(Bad_Record_Into_Mysql.super.getClass)

  override def invoke(value: (AzKafkaBuidData,Int,String), context: SinkFunction.Context[_]): Unit = {
    this.synchronized {

      logger.info("逻辑判断工单是未完工状态，数据输出到Mysql前先删除该工单")

      /**
        * CREATE TABLE `install_dirtydata` (
        * `id` varchar(100) DEFAULT NULL COMMENT '各表数据的主键id',
        * `pgguid` varchar(100) DEFAULT NULL COMMENT '工单单号',
        * `tablename` varchar(100) DEFAULT NULL COMMENT '数据来源表名称',
        * `ts` varchar(100) DEFAULT NULL COMMENT '数据产生时间戳',
        * `state` int(11) DEFAULT NULL COMMENT '数据状态1、0 其中0代表无需处理，1代表需要处理',
        * `data_describe` varchar(200) DEFAULT NULL COMMENT '数据丢弃具体信息描述',
        * KEY `id_index` (`id`),
        * KEY `pgguid_index` (`pgguid`),
        * KEY `state_index` (`state`)
        * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='安装工单丢弃数据明细';
        */

      //在添加
      try {
        logger.info("安装要插入的pgguid为->"+value._1.pgguid)
        JDBCUtil.executeUpdate("replace into install_dirtydata values(?,?,?,?,?,?)",
          Array(
            value._1.id,
            value._1.pgguid,
            value._1.table,
            value._1.ts,
            value._2,
            value._3
          ))
      } catch {
        case e: Exception => logger.error("安装脏数据输入，数据输出到Mysql失败..MYSQL异常信息->" + e.getMessage)
      }

    }
  }
}
