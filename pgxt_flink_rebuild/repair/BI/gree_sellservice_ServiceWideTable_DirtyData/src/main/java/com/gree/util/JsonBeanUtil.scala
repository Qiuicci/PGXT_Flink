package com.gree.util

import java.util

import com.gree.model.{MaxKuanBiao, WxDataChaoShi}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 将表数据转化为Json放入ES中
 */

object JsonBeanUtil {

  val logger: Logger = LoggerFactory.getLogger(JsonBeanUtil.getClass)
  private val numberFormatUtil = new NumberFormatUtil()

  def getWxkbTimeoutOrdersJson(wxDataChaoShi: WxDataChaoShi): util.HashMap[String, Any] = {
    this.synchronized {
      val json = new java.util.HashMap[String, Any]
      try {
        json.put("pgid", numberFormatUtil.panduanLong(wxDataChaoShi.pgid))
        json.put("created_by", wxDataChaoShi.created_by)
        json.put("created_date", wxDataChaoShi.created_date)
        json.put("last_modified_by", wxDataChaoShi.last_modified_by)
        json.put("last_modified_date", wxDataChaoShi.last_modified_date)
        json.put("auth_state", wxDataChaoShi.AUTH_STATE)
        json.put("azsl", numberFormatUtil.panduanInt(wxDataChaoShi.azsl))
        json.put("pgguid", wxDataChaoShi.pgguid)
        json.put("beiz", wxDataChaoShi.beiz)
        json.put("bjustat", numberFormatUtil.panduanInt(wxDataChaoShi.bjustat))
        json.put("chaoshiqe", wxDataChaoShi.chaoshiqe)
        json.put("cjdt", numberFormatUtil.panduanDate(wxDataChaoShi.cjdt))
        json.put("cjren", wxDataChaoShi.cjren)
        json.put("cjrmc", wxDataChaoShi.cjrmc)
        json.put("cjwdno", wxDataChaoShi.cjwdno)
        json.put("cshi", wxDataChaoShi.cshi)
        json.put("cxyzm", wxDataChaoShi.cxyzm)
        json.put("dhhm", wxDataChaoShi.dhhm)
        json.put("dizi", wxDataChaoShi.dizi)
        json.put("dqjdsj", wxDataChaoShi.dqjdsj)
        json.put("email", wxDataChaoShi.email)
        json.put("fjhm", wxDataChaoShi.fjhm)
        json.put("fwrybwgsj", wxDataChaoShi.fwrybwgsj)
        json.put("gdhao", wxDataChaoShi.gdhao)
        json.put("gpsdzxx", wxDataChaoShi.gpsdzxx)
        json.put("jindu", wxDataChaoShi.jindu)
        json.put("ldcs", numberFormatUtil.panduanInt(wxDataChaoShi.ldcs))
        json.put("qqlymc", wxDataChaoShi.qqlymc)
        json.put("qqlyxh", wxDataChaoShi.qqlyxh)
        json.put("qqlyzj", wxDataChaoShi.qqlyzj)
        json.put("quhao", wxDataChaoShi.quhao)
        json.put("qwsmjssj", wxDataChaoShi.qwsmjssj)
        json.put("qystat", wxDataChaoShi.qystat)
        json.put("sfen", wxDataChaoShi.sfen)
        json.put("sffswx", wxDataChaoShi.sffswx)
        json.put("spid", wxDataChaoShi.spid)
        json.put("spmc", wxDataChaoShi.spmc)
        json.put("ssqy", wxDataChaoShi.ssqy)
        json.put("stat", numberFormatUtil.panduanInt(wxDataChaoShi.stat))
        json.put("tsdengji", wxDataChaoShi.tsdengji)
        json.put("wcsj", wxDataChaoShi.wcsj)
        json.put("weidu", wxDataChaoShi.weidu)
        json.put("wwsl", numberFormatUtil.panduanInt(wxDataChaoShi.wwsl))
        json.put("wxren", wxDataChaoShi.wxren)
        json.put("wxrenid", wxDataChaoShi.wxrenid)
        json.put("wxshul", numberFormatUtil.panduanInt(wxDataChaoShi.wxshul))
        json.put("wxwdmc", wxDataChaoShi.wxwdmc)
        json.put("wxwdno", wxDataChaoShi.wxwdno)
        json.put("xian", wxDataChaoShi.xian)
        json.put("xjwdmc", wxDataChaoShi.xjwdmc)
        json.put("xjwdno", wxDataChaoShi.xjwdno)
        json.put("xjwdsj", wxDataChaoShi.xjwdsj)
        json.put("xqxiaolei", wxDataChaoShi.xqxiaolei)
        json.put("xsdh", wxDataChaoShi.xsdh)
        json.put("xsorsh", wxDataChaoShi.xsorsh)
        json.put("xswdmc", wxDataChaoShi.xswdmc)
        json.put("xswdno", wxDataChaoShi.xswdno)
        json.put("xxlb", wxDataChaoShi.xxlb)
        json.put("xxly", wxDataChaoShi.xxly)
        json.put("xxqd", wxDataChaoShi.xxqd)
        json.put("xzhen", wxDataChaoShi.xzhen)
        json.put("yddh", wxDataChaoShi.yddh)
        json.put("yddh2", wxDataChaoShi.yddh2)
        json.put("yhgyhf", numberFormatUtil.panduanInt(wxDataChaoShi.yhgyhf))
        json.put("yhif", wxDataChaoShi.yhif)
        json.put("yhmc", wxDataChaoShi.yhmc)
        json.put("yhqwsmsj", wxDataChaoShi.yhqwsmsj)
        json.put("yhsx", wxDataChaoShi.yhsx)
        json.put("yhyyczsj", wxDataChaoShi.yhyyczsj)
        json.put("yxji", wxDataChaoShi.yxji)
        json.put("zbby", wxDataChaoShi.zbby)
        json.put("zjczsj", wxDataChaoShi.zjczsj)
        json.put("zjczwd", wxDataChaoShi.zjczwd)
        json.put("zjczwdxtbh", wxDataChaoShi.zjczwdxtbh)
        json.put("zptype", wxDataChaoShi.zptype)
        json.put("zxhao", wxDataChaoShi.zxhao)
        json.put("cshiid", wxDataChaoShi.cshiid)
        json.put("sfenid", wxDataChaoShi.sfenid)
        json.put("xianid", wxDataChaoShi.xianid)
        json.put("xxlbid", wxDataChaoShi.xxlbid)
        json.put("xxlyid", wxDataChaoShi.xxlyid)
        json.put("xxqdid", wxDataChaoShi.xxqdid)
        json.put("yhsxid", wxDataChaoShi.yhsxid)
        json.put("xzhenid", wxDataChaoShi.xzhenid)
        json.put("wxcount", wxDataChaoShi.wxcount)
        json.put("extjson1", wxDataChaoShi.extjson1)
        json.put("extjson2", wxDataChaoShi.extjson2)
        json.put("extjson3", wxDataChaoShi.extjson3)
        json.put("extjson4", wxDataChaoShi.extjson4)
        json.put("extjson5", wxDataChaoShi.extjson5)
        json.put("appointmentjssj", numberFormatUtil.panduanDate(wxDataChaoShi.appointmentjssj))
        json.put("appointmentkssj", numberFormatUtil.panduanDate(wxDataChaoShi.appointmentkssj))
        json.put("yqwangongtime", numberFormatUtil.panduanDate(wxDataChaoShi.yqwangongtime))
        json.put("chaoshishichang", wxDataChaoShi.chaoshishichang)
        json.put("level1", wxDataChaoShi.level1)
        json.put("level2", wxDataChaoShi.level2)
        json.put("level3", wxDataChaoShi.level3)
        json.put("level4", wxDataChaoShi.level4)
        json.put("level5", wxDataChaoShi.level5)
        json.put("level6", wxDataChaoShi.level6)
        json.put("level7", wxDataChaoShi.level7)
        json.put("level8", wxDataChaoShi.level8)
        json.put("level9", wxDataChaoShi.level9)
        json.put("level10", wxDataChaoShi.level10)
        json.put("level11", wxDataChaoShi.level11)
        json.put("level12", wxDataChaoShi.level12)
        json.put("level13", wxDataChaoShi.level13)
        json.put("level14", wxDataChaoShi.level14)
      } catch {
        case e: Exception => logger.error("数据输出到超时表异常->" + e.getMessage)
      }
      json
    }
  }

  def getWxkbMaxBigKuanBiaoJson(maxKuanBiao: MaxKuanBiao): util.HashMap[String, Any] = {

    this.synchronized {
      val json = new java.util.HashMap[String, Any]
      try {

        json.put("pgid", maxKuanBiao.pgid)
        json.put("created_by", maxKuanBiao.created_by)
        json.put("created_date", maxKuanBiao.created_date)
        json.put("last_modified_by", maxKuanBiao.last_modified_by)
        json.put("last_modified_date", maxKuanBiao.last_modified_date)
        json.put("auth_state", maxKuanBiao.AUTH_STATE)
        json.put("azsl", numberFormatUtil.panduanInt(maxKuanBiao.azsl))
        json.put("pgguid", maxKuanBiao.pgguid)
        json.put("beiz", maxKuanBiao.beiz)
        json.put("bjustat", numberFormatUtil.panduanInt(maxKuanBiao.bjustat))
        json.put("chaoshiqe", maxKuanBiao.chaoshiqe)
        json.put("cjdt", numberFormatUtil.panduanDate(maxKuanBiao.cjdt))
        json.put("cjren", maxKuanBiao.cjren)
        json.put("cjrmc", maxKuanBiao.cjrmc)
        json.put("cjwdno", maxKuanBiao.cjwdno)
        json.put("cshi", maxKuanBiao.cshi)
        json.put("cshiid", maxKuanBiao.cshiid)
        json.put("cxyzm", maxKuanBiao.cxyzm)
        json.put("dhhm", maxKuanBiao.dhhm)
        json.put("dizi", maxKuanBiao.dizi)
        json.put("dqjdsj", maxKuanBiao.dqjdsj)
        json.put("email", maxKuanBiao.email)
        json.put("fjhm", maxKuanBiao.fjhm)
        json.put("fwrybwgsj", maxKuanBiao.fwrybwgsj)
        json.put("gdhao", maxKuanBiao.gdhao)
        json.put("gpsdzxx", maxKuanBiao.gpsdzxx)
        json.put("jindu", maxKuanBiao.jindu)
        json.put("ldcs", numberFormatUtil.panduanInt(maxKuanBiao.ldcs))
        json.put("qqlymc", maxKuanBiao.qqlymc)
        json.put("qqlyxh", maxKuanBiao.qqlyxh)
        json.put("qqlyzj", maxKuanBiao.qqlyzj)
        json.put("quhao", maxKuanBiao.quhao)
        json.put("qwsmjssj", maxKuanBiao.qwsmjssj)
        json.put("qystat", maxKuanBiao.qystat)
        json.put("sfen", maxKuanBiao.sfen)
        json.put("sfenid", maxKuanBiao.sfenid)
        json.put("sffswx", maxKuanBiao.sffswx)
        json.put("spid", maxKuanBiao.spid)
        json.put("spmc", maxKuanBiao.spmc)
        json.put("ssqy", maxKuanBiao.ssqy)
        json.put("stat", numberFormatUtil.panduanInt(maxKuanBiao.stat))
        json.put("tsdengji", maxKuanBiao.tsdengji)
        json.put("wcsj", maxKuanBiao.wcsj)
        json.put("weidu", maxKuanBiao.weidu)
        json.put("wwsl", numberFormatUtil.panduanInt(maxKuanBiao.wwsl))
        json.put("wxren", maxKuanBiao.wxren)
        json.put("wxrenid", maxKuanBiao.wxrenid)
        json.put("wxshul", numberFormatUtil.panduanInt(maxKuanBiao.wxshul))
        json.put("wxwdmc", maxKuanBiao.wxwdmc)
        json.put("wxwdno", maxKuanBiao.wxwdno)
        json.put("xian", maxKuanBiao.xian)
        json.put("xianid", maxKuanBiao.xianid)
        json.put("xjwdmc", maxKuanBiao.xjwdmc)
        json.put("xjwdno", maxKuanBiao.xjwdno)
        json.put("xjwdsj", maxKuanBiao.xjwdsj)
        json.put("xqxiaolei", maxKuanBiao.xqxiaolei)
        json.put("xsdh", maxKuanBiao.xsdh)
        json.put("xsorsh", maxKuanBiao.xsorsh)
        json.put("xswdmc", maxKuanBiao.xswdmc)
        json.put("xswdno", maxKuanBiao.xswdno)
        json.put("xxlb", maxKuanBiao.xxlb)
        json.put("xxlbid", maxKuanBiao.xxlbid)
        json.put("xxly", maxKuanBiao.xxly)
        json.put("xxlyid", maxKuanBiao.xxlyid)
        json.put("xxqd", maxKuanBiao.xxqd)
        json.put("xxqdid", maxKuanBiao.xxqdid)
        json.put("xzhen", maxKuanBiao.xzhen)
        json.put("yddh", maxKuanBiao.yddh)
        json.put("yddh2", maxKuanBiao.yddh2)
        json.put("yhgyhf", numberFormatUtil.panduanInt(maxKuanBiao.yhgyhf))
        json.put("yhif", maxKuanBiao.yhif)
        json.put("yhmc", maxKuanBiao.yhmc)
        json.put("yhqwsmsj", maxKuanBiao.yhqwsmsj)
        json.put("yhsx", maxKuanBiao.yhsx)
        json.put("yhyyczsj", maxKuanBiao.yhyyczsj)
        json.put("yxji", maxKuanBiao.yxji)
        json.put("zbby", maxKuanBiao.zbby)
        json.put("zjczsj", maxKuanBiao.zjczsj)
        json.put("zjczwd", maxKuanBiao.zjczwd)
        json.put("zjczwdxtbh", maxKuanBiao.zjczwdxtbh)
        json.put("zptype", maxKuanBiao.zptype)
        json.put("zxhao", maxKuanBiao.zxhao)
        json.put("yhsxid", maxKuanBiao.yhsxid)
        json.put("xzhenid", maxKuanBiao.xzhenid)
        json.put("tbl_assign_daijian", maxKuanBiao.daiJianBiaoArray)
        json.put("tbl_assign_feedback", maxKuanBiao.fanKuiBiaoArray)
        json.put("tbl_assign_fkmx", maxKuanBiao.fkmxBiaoArray)
        json.put("tbl_assign_satisfaction", maxKuanBiao.manYiBiaoArray)
        json.put("tbl_assign_mx", maxKuanBiao.mingXiBiaoArray)
        json.put("tbl_assign_xzyd", maxKuanBiao.xinXinXiBiaoArray)
        json.put("tbl_assign_appointment", maxKuanBiao.yuYueBiaoArray)
        json.put("level1", maxKuanBiao.level1)
        json.put("level2", maxKuanBiao.level2)
        json.put("level3", maxKuanBiao.level3)
        json.put("level4", maxKuanBiao.level4)
        json.put("level5", maxKuanBiao.level5)
        json.put("level6", maxKuanBiao.level6)
        json.put("level7", maxKuanBiao.level7)
        json.put("level8", maxKuanBiao.level8)
        json.put("level9", maxKuanBiao.level9)
        json.put("level10", maxKuanBiao.level10)
        json.put("level11", maxKuanBiao.level11)
        json.put("level12", maxKuanBiao.level12)
        json.put("level13", maxKuanBiao.level13)
        json.put("level14", maxKuanBiao.level14)
        json.put("appointmentkssj", numberFormatUtil.panduanDate(maxKuanBiao.appointmentkssj))
        json.put("appointmentjssj", numberFormatUtil.panduanDate(maxKuanBiao.appointmentjssj))
        json.put("yqwangongtime", numberFormatUtil.panduanDate(maxKuanBiao.yqwangongtime))
        json.put("fkmx_fklb", maxKuanBiao.fklb)
        json.put("fkmx_fkjg", maxKuanBiao.fkjg)
        json.put("fkmx_fksj", maxKuanBiao.fksj)
        json.put("unReadXxx", numberFormatUtil.panduanLong(maxKuanBiao.unReadXxx))
        json.put("wxcount", maxKuanBiao.wxcount)
        json.put("extjson1", maxKuanBiao.extjson1)
        json.put("extjson2", maxKuanBiao.extjson2)
        json.put("extjson3", maxKuanBiao.extjson3)
        json.put("extjson4", maxKuanBiao.extjson4)
        json.put("extjson5", maxKuanBiao.extjson5)
        json.put("isOrNotDaiJian",maxKuanBiao.isOrNotDaiJian)
        json.put("baowangongtime",maxKuanBiao.baowangongtime)
        json.put("vip",maxKuanBiao.vip)
      } catch {
        case e: Exception => logger.error("数据输出到WXKB超大宽表异常->" + e.getMessage)
      }
      json
    }
  }
}
