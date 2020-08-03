package com.gree.func

import com.gree.model.AzDataChaoShi
import com.gree.util.{JDBCUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class WeiWanGongChaoShiToMySqlSink extends RichSinkFunction[AzDataChaoShi]{
  val logger: Logger = LoggerFactory.getLogger(WeiWanGongChaoShiToMySqlSink.super.getClass)


  override def invoke(value: AzDataChaoShi, context: SinkFunction.Context[_]): Unit = {
    this.synchronized {

      val util = new NumberFormatUtil

      logger.info("逻辑判断工单是未完工状态，数据输出到Mysql前先删除该工单")

      //在添加
      try {
        logger.info("安装要插入的pgguid为->"+value.pgguid)
        JDBCUtil.executeUpdate("replace into az_wait_timeout_orders1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
          Array(
            value.pgguid,
            value.created_by,
            util.panduanDate(value.created_date),
            value.last_modified_by,
            util.panduanDate(value.last_modified_date),
            value.pgid,
            value.yhmc,
            value.yddh,
            value.yddh2,
            value.quhao,
            value.dhhm,
            value.fjhm,
            value.email,
            value.sfen,
            value.cshi,
            value.xian,
            value.xzhen,
            value.dizi,
            value.xxqd,
            value.xxly,
            value.xxlb,
            value.beiz,
            value.yhsx,
            value.yxji,
            value.gdhao,
            value.spid,
            value.spmc,
            value.azren,
            value.azrenid,
            value.azwdxtbh,
            value.azwdno,
            value.azwdmc,
            value.jspgwdno,
            value.jspgwdmc,
            value.jspgwdxtbh,
            util.panduanDate(value.jspgwdsj),
            value.zxha,
            value.ssqy,
            value.qqlyno,
            value.qqlymc,
            value.qqlyxh,
            value.qqlyzj,
            util.panduanInt(value.bjustat),
            util.panduanDate(value.yhqwkssj),
            util.panduanDate(value.yhqwjssj),
            util.panduanInt(value.stat),
            value.gpsdzxx,
            value.cjren,
            value.cjrmc,
            util.panduanDate(value.cjdt),
            value.cjwdno,
            value.cjwdxtbh,
            value.zjczren,
            value.zjczwd,
            value.zjczwdxtbh,
            util.panduanDate(value.zjczsj),
            value.xslx,
            value.lcid,
            value.djlxno,
            util.panduanDate(value.yyazsj),
            value.sfwcps,
            value.xsdh,
            value.gcbh,
            value.gcmc,
            util.panduanInt(value.azsl),
            util.panduanInt(value.wwsl),
            value.lxren,
            value.xsdwno,
            value.xswdmc,
            value.xsdwxtbh,
            value.fphm,
            util.panduanDate(value.gmsj),
            value.kqbh,
            util.panduanInt(value.xsorsh),
            util.panduanDate(value.dqjdsj),
            value.syjd,
            util.panduanLong(value.dqjd),
            value.jindu,
            value.weidu,
            util.panduanDate(value.shsj),
            util.panduanInt(value.sfygllc),
            value.xslxid,
            value.yhsxid,
            value.xxlbid,
            value.xxlyid,
            value.sfenid,
            value.cshiid,
            value.xianid,
            value.xzhenid,
            value.cljggz,
            util.panduanDate(value.appointmentkssj),
            util.panduanDate(value.appointmentjssj),
            util.panduanDate(value.yqwangongtime),
            value.chaoshishichang,
            util.panduanDate(value.shijiwangongtime),
            value.level1,
            value.level2,
            value.level3,
            value.level4,
            value.level5,
            value.level6,
            value.level7,
            value.level8,
            value.level9,
            value.level10,
            value.level11,
            value.level12,
            value.level13,
            value.level14,
            util.panduanDate(value.wcsj),
            util.panduanInt1(value.retailsign),
            value.servicewdmc,
            value.shopname,
            value.orderphone,
            value.extendfiled1,
            value.extendfiled2,
            value.extendfiled3,
            value.extendfiled4,
            value.servicewdno,
            value.shopno,
            value.extendfiled5,
            value.organizationaddress,
            value.organizationname
          ))
      } catch {
        case e: Exception => logger.error("未完工状态，数据输出到Mysql失败..MYSQL异常信息->" + e.getMessage)
      }

    }
  }


}
