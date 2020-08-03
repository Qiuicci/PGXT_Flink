package com.gree.func

import com.gree.model.WxDataChaoShi
import com.gree.util.{JDBCUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class WeiWanGongChaoShiToMySqlSink extends  RichSinkFunction[WxDataChaoShi]{
  val logger: Logger = LoggerFactory.getLogger(WeiWanGongChaoShiToMySqlSink.super.getClass)

  override def invoke(value: WxDataChaoShi, context: SinkFunction.Context[_]): Unit = {
    this.synchronized{
      val util = new NumberFormatUtil

      logger.info("逻辑判断工单是未完工状态，数据输出到Mysql前先删除该工单")

      //在添加
      try {
        logger.info("要插入的pgid为->"+value.pgid)
        JDBCUtil.executeUpdate1("replace into wx_wait_timeout_orders1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
          Array(
            util.panduanLong(value.pgid),
            value.created_by,
            util.panduanDate(value.created_date),
            value.last_modified_by,
            util.panduanDate(value.last_modified_date),
            value.AUTH_STATE,
            util.panduanInt(value.azsl),
            value.beiz,
            util.panduanInt(value.bjustat),
            value.chaoshiqe,
            util.panduanDate(value.cjdt),
            value.cjren,
            value.cjrmc,
            value.cjwdno,
            value.cshi,
            value.cxyzm,
            value.dhhm,
            value.dizi,
            util.panduanDate(value.dqjdsj),
            value.email,
            value.fjhm,
            util.panduanDate(value.fwrybwgsj),
            value.gdhao,
            value.gpsdzxx,
            value.jindu,
            util.panduanInt(value.ldcs),
            value.pgguid,
            value.qqlymc,
            value.qqlyxh,
            value.qqlyzj,
            value.quhao,
            util.panduanDate(value.qwsmjssj),
            value.qystat,
            value.sfen,
            value.sffswx,
            value.spid,
            value.spmc,
            value.ssqy,
            util.panduanInt(value.stat),
            value.tsdengji,
            util.panduanDate(value.wcsj),
            value.weidu,
            util.panduanInt(value.wwsl),
            value.wxren,
            value.wxrenid,
            util.panduanInt(value.wxshul),
            value.wxwdmc,
            value.wxwdno,
            value.xian,
            value.xjwdmc,
            value.xjwdno,
            util.panduanDate(value.xjwdsj),
            value.xqxiaolei,
            value.xsdh,
            util.panduanInt(value.xsorsh),
            value.xswdmc,
            value.xswdno,
            value.xxlb,
            value.xxly,
            value.xxqd,
            value.xzhen,
            value.yddh,
            value.yddh2,
            util.panduanInt(value.yhgyhf),
            value.yhif,
            value.yhmc,
            util.panduanDate(value.yhqwsmsj),
            value.yhsx,
            util.panduanDate(value.yhyyczsj),
            value.yxji,
            util.panduanNull(value.zbby),
            util.panduanDate(value.zjczsj),
            value.zjczwd,
            value.zjczwdxtbh,
            value.zptype,
            value.zxhao,
            value.cshiid,
            value.sfenid,
            value.xianid,
            value.xxlbid,
            value.xxlyid,
            value.xxqdid,
            value.yhsxid,
            util.panduanDate(value.appointmentkssj),
            util.panduanDate(value.appointmentjssj),
            util.panduanDate(value.yqwangongtime),
            value.chaoshishichang,
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
            value.xzhenid,
            value.wxcount,
            value.extjson1,
            value.extjson2,
            value.extjson3,
            value.extjson4,
            value.extjson5
          ))
      } catch {
        case e: Exception => logger.error("未完工状态，数据输出到Mysql失败..MYSQL异常信息->" + e.getMessage)
      }
    }
  }
}
