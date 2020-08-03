package com.gree.util

import java.util

import com.gree.model._
import org.slf4j.{Logger, LoggerFactory}

/**
 * 将表数据转化为Json放入ES中
 */

object JsonBeanUtil {

  val logger: Logger = LoggerFactory.getLogger(JsonBeanUtil.getClass)
  private val numberFormatUtil = new NumberFormatUtil()
  def getTblAssignJson(TBL_ASSIGN: Tbl_Assign): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("pgid", TBL_ASSIGN.pgid)
      json.put("created_by", TBL_ASSIGN.created_by)
      json.put("created_date", TBL_ASSIGN.created_date)
      json.put("last_modified_by", TBL_ASSIGN.last_modified_by)
      json.put("last_modified_date", TBL_ASSIGN.last_modified_date)
      json.put("auth_state", TBL_ASSIGN.AUTH_STATE)
      json.put("azsl", numberFormatUtil.panduanInt(TBL_ASSIGN.azsl))
      json.put("pgguid", TBL_ASSIGN.pgguid)
      json.put("beiz", TBL_ASSIGN.beiz)
      json.put("bjustat", numberFormatUtil.panduanInt(TBL_ASSIGN.bjustat))
      json.put("chaoshiqe", TBL_ASSIGN.chaoshiqe)
      json.put("cjdt", numberFormatUtil.panduanDate(TBL_ASSIGN.cjdt))
      json.put("cjren", TBL_ASSIGN.cjren)
      json.put("cjrmc", TBL_ASSIGN.cjrmc)
      json.put("cjwdno", TBL_ASSIGN.cjwdno)
      json.put("cshi", TBL_ASSIGN.cshi)
      json.put("cshiid", TBL_ASSIGN.cshiid)
      json.put("cxyzm", TBL_ASSIGN.cxyzm)
      json.put("dhhm", TBL_ASSIGN.dhhm)
      json.put("dizi", TBL_ASSIGN.dizi)
      json.put("dqjdsj", TBL_ASSIGN.dqjdsj)
      json.put("email", TBL_ASSIGN.email)
      json.put("fjhm", TBL_ASSIGN.fjhm)
      json.put("fwrybwgsj", TBL_ASSIGN.fwrybwgsj)
      json.put("gdhao", TBL_ASSIGN.gdhao)
      json.put("gpsdzxx", TBL_ASSIGN.gpsdzxx)
      json.put("jindu", TBL_ASSIGN.jindu)
      json.put("ldcs", numberFormatUtil.panduanInt(TBL_ASSIGN.ldcs))
      json.put("qqlymc", TBL_ASSIGN.qqlymc)
      json.put("qqlyxh", TBL_ASSIGN.qqlyxh)
      json.put("qqlyzj", TBL_ASSIGN.qqlyzj)
      json.put("quhao", TBL_ASSIGN.quhao)
      json.put("qwsmjssj", TBL_ASSIGN.qwsmjssj)
      json.put("qystat", TBL_ASSIGN.qystat)
      json.put("sfen", TBL_ASSIGN.sfen)
      json.put("sfenid", TBL_ASSIGN.sfenid)
      json.put("sffswx", TBL_ASSIGN.sffswx)
      json.put("spid", TBL_ASSIGN.spid)
      json.put("spmc", TBL_ASSIGN.spmc)
      json.put("ssqy", TBL_ASSIGN.ssqy)
      json.put("stat", numberFormatUtil.panduanInt(TBL_ASSIGN.stat))
      json.put("tsdengji", TBL_ASSIGN.tsdengji)
      json.put("wcsj", TBL_ASSIGN.wcsj)
      json.put("weidu", TBL_ASSIGN.weidu)
      json.put("wwsl",numberFormatUtil.panduanInt(TBL_ASSIGN.wwsl))
      json.put("wxren", TBL_ASSIGN.wxren)
      json.put("wxrenid", TBL_ASSIGN.wxrenid)
      json.put("wxshul",numberFormatUtil.panduanInt(TBL_ASSIGN.wxshul))
      json.put("wxwdmc", TBL_ASSIGN.wxwdmc)
      json.put("wxwdno", TBL_ASSIGN.wxwdno)
      json.put("xian", TBL_ASSIGN.xian)
      json.put("xianid", TBL_ASSIGN.xianid)
      json.put("xjwdmc", TBL_ASSIGN.xjwdmc)
      json.put("xjwdno", TBL_ASSIGN.xjwdno)
      json.put("xjwdsj", TBL_ASSIGN.xjwdsj)
      json.put("xqxiaolei", TBL_ASSIGN.xqxiaolei)
      json.put("xsdh", TBL_ASSIGN.xsdh)
      json.put("xsorsh", TBL_ASSIGN.xsorsh)
      json.put("xswdmc", TBL_ASSIGN.xswdmc)
      json.put("xswdno", TBL_ASSIGN.xswdno)
      json.put("xxlb", TBL_ASSIGN.xxlb)
      json.put("xxlbid", TBL_ASSIGN.xxlbid)
      json.put("xxly", TBL_ASSIGN.xxly)
      json.put("xxlyid", TBL_ASSIGN.xxlyid)
      json.put("xxqd", TBL_ASSIGN.xxqd)
      json.put("xxqdid", TBL_ASSIGN.xxqdid)
      json.put("xzhen", TBL_ASSIGN.xzhen)
      json.put("yddh", TBL_ASSIGN.yddh)
      json.put("yddh2", TBL_ASSIGN.yddh2)
      json.put("yhgyhf",numberFormatUtil.panduanInt(TBL_ASSIGN.yhgyhf))
      json.put("yhif", TBL_ASSIGN.yhif)
      json.put("yhmc", TBL_ASSIGN.yhmc)
      json.put("yhqwsmsj", TBL_ASSIGN.yhqwsmsj)
      json.put("yhsx", TBL_ASSIGN.yhsx)
      json.put("yhyyczsj", TBL_ASSIGN.yhyyczsj)
      json.put("yxji", TBL_ASSIGN.yxji)
      json.put("zbby", TBL_ASSIGN.zbby)
      json.put("zjczsj", TBL_ASSIGN.zjczsj)
      json.put("zjczwd", TBL_ASSIGN.zjczwd)
      json.put("zjczwdxtbh", TBL_ASSIGN.zjczwdxtbh)
      json.put("zptype", TBL_ASSIGN.zptype)
      json.put("zxhao", TBL_ASSIGN.zxhao)
      json.put("yhsxid", TBL_ASSIGN.yhsxid)
      json.put("xzhenid", TBL_ASSIGN.xzhenid)
      json.put("wxcount", TBL_ASSIGN.wxcount)
      json.put("extjson1", TBL_ASSIGN.extjson1)
      json.put("extjson2", TBL_ASSIGN.extjson2)
      json.put("extjson3", TBL_ASSIGN.extjson3)
      json.put("extjson4", TBL_ASSIGN.extjson4)
      json.put("extjson5", TBL_ASSIGN.extjson5)
      json.put("ts", TBL_ASSIGN.ts)
      json.put("vip",TBL_ASSIGN.vip)
    } catch {
      case e: Exception => logger.error("同步主表出现异常" + e.getMessage)
    }
    json
  }

  def getTblAssignAppointmentJson(Tbl_ASSIGN_APPOINTMENT: Tbl_Assign_Appointment): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id", Tbl_ASSIGN_APPOINTMENT.id)
      json.put("created_by", Tbl_ASSIGN_APPOINTMENT.created_by)
      json.put("created_date", Tbl_ASSIGN_APPOINTMENT.created_date)
      json.put("last_modified_by", Tbl_ASSIGN_APPOINTMENT.last_modified_by)
      json.put("last_modified_date", Tbl_ASSIGN_APPOINTMENT.last_modified_date)
      json.put("beiz", Tbl_ASSIGN_APPOINTMENT.beiz)
      json.put("czren", Tbl_ASSIGN_APPOINTMENT.czren)
      json.put("czsj", numberFormatUtil.panduanDate(Tbl_ASSIGN_APPOINTMENT.czsj))
      json.put("jssj", numberFormatUtil.panduanDate(Tbl_ASSIGN_APPOINTMENT.jssj))
      json.put("kssj", numberFormatUtil.panduanDate(Tbl_ASSIGN_APPOINTMENT.kssj))
      json.put("leix", Tbl_ASSIGN_APPOINTMENT.leix)
      json.put("pgid", Tbl_ASSIGN_APPOINTMENT.pgid)
      json.put("reason", Tbl_ASSIGN_APPOINTMENT.reason)
      json.put("ts", Tbl_ASSIGN_APPOINTMENT.ts)
    } catch {
      case e: Exception => logger.error("同步预约表出现异常" + e.getMessage)
    }
    json
  }

  def getTblAssignFkmxJson(tblAssignFkmx: Tbl_Assign_Fkmx): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("fkid", tblAssignFkmx.fkid)
      json.put("created_by", tblAssignFkmx.created_by)
      json.put("created_date", tblAssignFkmx.created_date)
      json.put("last_modified_by", tblAssignFkmx.last_modified_by)
      json.put("last_modified_date", tblAssignFkmx.last_modified_date)
      json.put("pgid", tblAssignFkmx.pgid)
      json.put("fklb", tblAssignFkmx.fklb)
      json.put("fkjg", tblAssignFkmx.fkjg)
      json.put("fknr", tblAssignFkmx.fknr)
      json.put("fkren", tblAssignFkmx.fkren)
      json.put("fkrenmc", tblAssignFkmx.fkren)
      json.put("fksj", numberFormatUtil.panduanDate(tblAssignFkmx.fksj))
      json.put("fkwdno", tblAssignFkmx.fkwdno)
      json.put("fkwdmc", tblAssignFkmx.fkwdmc)
      json.put("scid", tblAssignFkmx.scid)
      json.put("scwj", tblAssignFkmx.scwj)
      json.put("qqlyxh", tblAssignFkmx.qqlyxh)
      json.put("fkmxguid", tblAssignFkmx.fkmxguid)
      json.put("wjid", tblAssignFkmx.wjid)
      json.put("ts", tblAssignFkmx.ts)
    } catch {
      case e: Exception => logger.error("同步反馈明细表出现异常" + e.getMessage)
    }
    json
  }

  def getTblAssignXzydJson(tbl_Assign_Xzyd: Tbl_Assign_Xzyd): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
       json.put("xzid", tbl_Assign_Xzyd.xzid)
        json.put("created_by", tbl_Assign_Xzyd.created_by)
        json.put("created_date", tbl_Assign_Xzyd.created_date)
        json.put("last_modified_by", tbl_Assign_Xzyd.last_modified_by)
        json.put("last_modified_date", tbl_Assign_Xzyd.last_modified_date)
        json.put("pgid", tbl_Assign_Xzyd.pgid)
        json.put("czren", tbl_Assign_Xzyd.czren)
        json.put("czsj", tbl_Assign_Xzyd.czsj)
        json.put("wdno", tbl_Assign_Xzyd.wdno)
        json.put("cshu", numberFormatUtil.panduanInt(tbl_Assign_Xzyd.cshu))
        json.put("xzyq", tbl_Assign_Xzyd.xzyq)
        json.put("xzyqlb", tbl_Assign_Xzyd.xzyqlb)
        json.put("ydbz", numberFormatUtil.panduanInt(tbl_Assign_Xzyd.ydbz))
        json.put("ydsj", tbl_Assign_Xzyd.ydsj)
        json.put("ydren", tbl_Assign_Xzyd.ydren)
        json.put("ydrmc", tbl_Assign_Xzyd.ydrmc)
        json.put("ydwd", tbl_Assign_Xzyd.ydwd)
        json.put("ydwdmc", tbl_Assign_Xzyd.ydwdmc)
      json.put("ts", tbl_Assign_Xzyd.ts)

    } catch {
      case e: Exception => logger.error("同步Tbl_Assign_Xzyd 阅读出现异常" + e.getMessage)
    }
    json
  }

  def getTblAssignMx(tblAssignMx:Tbl_Assign_Mx):util.HashMap[String, Any] ={
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("pgmxid", tblAssignMx.pgmxid)
      json.put("created_by", tblAssignMx.created_by)
      json.put("created_date", tblAssignMx.created_date)
      json.put("last_modified_by", tblAssignMx.last_modified_by)
      json.put("last_modified_date", tblAssignMx.last_modified_date)
      json.put("pgid", tblAssignMx.pgid)
      json.put("spid", tblAssignMx.spid)
      json.put("spmc", tblAssignMx.spmc)
      json.put("xlid", tblAssignMx.xlid)
      json.put("xlmc", tblAssignMx.xlmc)
      json.put("xiid", tblAssignMx.xiid)
      json.put("ximc", tblAssignMx.ximc)
      json.put("jxid", tblAssignMx.jxid)
      json.put("jxmc", tblAssignMx.jxmc)
      json.put("jxno", tblAssignMx.jxno)
      json.put("gmsj", tblAssignMx.gmsj)
      json.put("xsdw", tblAssignMx.xsdw)
      json.put("xsdwdh", tblAssignMx.xsdwdh)
      json.put("fwdw", tblAssignMx.fwdw)
      json.put("fwdwdh", tblAssignMx.fwdwdh)
      json.put("gzwz", tblAssignMx.gzwz)
      json.put("gzxx", tblAssignMx.gzxx)
      json.put("czren", tblAssignMx.czren)
      json.put("czsj", tblAssignMx.czsj)
      json.put("czwd", tblAssignMx.czwd)
      json.put("njtm", tblAssignMx.njtm)
      json.put("wjtm", tblAssignMx.wjtm)
      json.put("beiz", tblAssignMx.beiz)
      json.put("njtm2", tblAssignMx.njtm2)
      json.put("qqlyxh", tblAssignMx.qqlyxh)
      json.put("pinpai", tblAssignMx.pinpai)
      json.put("fee", numberFormatUtil.panduanDouble(tblAssignMx.fee))
      json.put("xxfee", numberFormatUtil.panduanDouble(tblAssignMx.xxfee))
      json.put("hsqk", tblAssignMx.hsqk)
      json.put("gzxxid", tblAssignMx.gzxxid)
      json.put("shul", numberFormatUtil.panduanInt(tblAssignMx.shul))
      json.put("wwsl", tblAssignMx.wwsl)
      json.put("ts", tblAssignMx.ts)
      json.put("wxcount",tblAssignMx.wxcount)
      json.put("tmjscount",tblAssignMx.tmjscount)
      json.put("yblength",tblAssignMx.yblength)
      json.put("bxdue",tblAssignMx.bxdue)
    } catch {
      case  e:Exception=>logger.error("明细表同步出现异常"+e.getMessage)
    }
    json
  }

  def getTblAssignSatisfaction(tblAssignSatisfaction:Tbl_Assign_Satisfaction):util.HashMap[String, Any] ={
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id", tblAssignSatisfaction.id)
      json.put("created_by", tblAssignSatisfaction.created_by)
      json.put("created_date", tblAssignSatisfaction.created_date)
      json.put("last_modified_by", tblAssignSatisfaction.last_modified_by)
      json.put("last_modified_date", tblAssignSatisfaction.last_modified_date)
      json.put("pgid", tblAssignSatisfaction.pgid)
      json.put("pjly", tblAssignSatisfaction.pjly)
      json.put("pjnr", tblAssignSatisfaction.pjnr)
      json.put("hfren", tblAssignSatisfaction.hfren)
      json.put("hfwdmc", tblAssignSatisfaction.hfwdmc)
      json.put("hfwdno", tblAssignSatisfaction.hfwdno)
      json.put("hfsj", tblAssignSatisfaction.hfsj)
      json.put("bmylx", tblAssignSatisfaction.bmylx)
      json.put("bmybeiz", tblAssignSatisfaction.bmybeiz)
      json.put("bmysj", tblAssignSatisfaction.bmysj)
      json.put("splb", tblAssignSatisfaction.splb)
      json.put("mydlx", numberFormatUtil.panduanInt(tblAssignSatisfaction.mydlx))
      json.put("sxlx", tblAssignSatisfaction.sxlx)
      json.put("ts", tblAssignSatisfaction.ts)
    } catch {
      case  e:Exception=> logger.error("满意表同步出现异常"+e.getMessage)
    }
    json
  }

  def getTblAssignFeedBack(tblAssignFeedBack:Tbl_Assign_FeedBack):util.HashMap[String, Any] ={
    val json = new java.util.HashMap[String, Any]

    try {
      json.put("id", tblAssignFeedBack.id)
      json.put("created_by", tblAssignFeedBack.created_by)
      json.put("created_date", tblAssignFeedBack.created_date)
      json.put("last_modified_by", tblAssignFeedBack.last_modified_by)
      json.put("last_modified_date", tblAssignFeedBack.last_modified_date)
      json.put("pgid", tblAssignFeedBack.pgid)
      json.put("zlfksj", numberFormatUtil.panduanDate(tblAssignFeedBack.zlfksj))
      json.put("zlfkbh", tblAssignFeedBack.zlfkbh)
      json.put("czren", tblAssignFeedBack.czren)
      json.put("czsj", tblAssignFeedBack.czsj)
      json.put("ts", tblAssignFeedBack.ts)

    } catch {
      case  e:Exception=>logger.error("反馈表同步出现异常"+e.getMessage)
    }
    json
  }

  def getTblAssignDaijian(tblAssignDaiJian:Tbl_Assign_DaiJian):util.HashMap[String, Any] ={
    val json = new java.util.HashMap[String, Any]

    try {
      json.put("id", tblAssignDaiJian.id)
      json.put("created_by", tblAssignDaiJian.created_by)
      json.put("created_date", tblAssignDaiJian.created_date)
      json.put("last_modified_by", tblAssignDaiJian.last_modified_by)
      json.put("last_modified_date", tblAssignDaiJian.last_modified_date)
      json.put("pjsqbh", tblAssignDaiJian.pjsqbh)
      json.put("pjsqsj", tblAssignDaiJian.pjsqsj)
      json.put("pjwlbm", tblAssignDaiJian.pjwlbm)
      json.put("pjwlmc", tblAssignDaiJian.pjwlmc)
      json.put("pjwlsl", tblAssignDaiJian.pjwlsl)
      json.put("djquyunum", numberFormatUtil.panduanInt(tblAssignDaiJian.djquyunum))
      json.put("djxsgsnum", numberFormatUtil.panduanInt(tblAssignDaiJian.djxsgsnum))
      json.put("djwdnum", numberFormatUtil.panduanInt(tblAssignDaiJian.djwdnum))
      json.put("pgid", tblAssignDaiJian.pgid)
      json.put("djwd", tblAssignDaiJian.djwd)
      json.put("djwdmc", tblAssignDaiJian.djwdmc)
      json.put("djsj", tblAssignDaiJian.djsj)
      json.put("splb", tblAssignDaiJian.splb)
      json.put("cjdt", numberFormatUtil.panduanDate(tblAssignDaiJian.cjdt))
      json.put("thwlbm", tblAssignDaiJian.thwlbm)
      json.put("thwlmc", tblAssignDaiJian.thwlmc)
      json.put("thbmxsgsnum", numberFormatUtil.panduanInt(tblAssignDaiJian.thbmxsgsnum))
      json.put("thbmqynum", numberFormatUtil.panduanInt(tblAssignDaiJian.thbmqynum))
      json.put("thbmwdnum", numberFormatUtil.panduanInt(tblAssignDaiJian.thbmwdnum))
      json.put("pjxtflag", numberFormatUtil.panduanInt(tblAssignDaiJian.pjxtflag))
      json.put("ts",tblAssignDaiJian.ts)

    } catch {
      case  e:Exception=> logger.error("待件表同步出现异常"+e.getMessage)
    }
    json
  }

}
