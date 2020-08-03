package com.gree.util

import java.util

import com.gree.model._
import org.slf4j.{Logger, LoggerFactory}


object JsonBeanUtil {
  private val numberFormatUtil = new NumberFormatUtil()
  private val logger: Logger = LoggerFactory.getLogger(JsonBeanUtil.getClass)

  //安装platform表
  def getTblAzAssignPlatformJson(tblAzAssignPlatform:Tbl_Az_Assign_Platform) : util.HashMap[String,Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",tblAzAssignPlatform.id)
      json.put("created_by",tblAzAssignPlatform.created_by)
      json.put("created_date",tblAzAssignPlatform.created_date)
      json.put("last_modified_by",tblAzAssignPlatform.last_modified_by)
      json.put("last_modified_date",tblAzAssignPlatform.last_modified_date)
      json.put("parentbizorderid",tblAzAssignPlatform.parentbizorderid)
      json.put("bizorderid",tblAzAssignPlatform.bizorderid)
      json.put("pgguid",tblAzAssignPlatform.pgguid)
      json.put("yywm",tblAzAssignPlatform.yywm)
      json.put("xdsj",tblAzAssignPlatform.xdsj)
      json.put("ts",tblAzAssignPlatform.ts)
      json.put("table",tblAzAssignPlatform.table)

    } catch {
      case e: Exception => logger.error("同步TblAzAssignPlatform表出现异常" + e.getMessage)
    }
    json
  }

  def getTblYjhxJddJson(tblYjhxJdd: Tbl_Yjhx_Jdd): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",tblYjhxJdd.id)
      json.put("created_by",tblYjhxJdd.created_by)
      json.put("created_date",tblYjhxJdd.created_date)
      json.put("last_modified_by",tblYjhxJdd.last_modified_by)
      json.put("last_modified_date",tblYjhxJdd.last_modified_date)
      json.put("oldMachineBrand",tblYjhxJdd.oldMachineBrand)
      json.put("oldMachineType",tblYjhxJdd.oldMachineType)
      json.put("brandFlag",numberFormatUtil.panduanInt(tblYjhxJdd.brandFlag))
      json.put("typeFlag",numberFormatUtil.panduanInt(tblYjhxJdd.typeFlag))
      json.put("realBrand",tblYjhxJdd.realBrand)
      json.put("realType",tblYjhxJdd.realType)
      json.put("machineIntegrity",tblYjhxJdd.machineIntegrity)
      json.put("tempBarcode",tblYjhxJdd.tempBarcode)
      json.put("tempBarcodeImg",tblYjhxJdd.tempBarcodeImg)
      json.put("identifyResult",tblYjhxJdd.identifyResult)
      json.put("hxjddid",tblYjhxJdd.hxjddid)
      json.put("pgguid",tblYjhxJdd.pgguid)
      json.put("xsdh",tblYjhxJdd.xsdh)
      json.put("cpps",tblYjhxJdd.cpps)
      json.put("ts",tblYjhxJdd.ts)
      json.put("table",tblYjhxJdd.table)

    } catch {
      case e: Exception => logger.error("同步TblYjhxJdd表出现异常" + e.getMessage)
    }
    json
  }

  def getTblAzTradeNewForOldJson(tblTradeNewForOld: Tbl_Trade_New_For_Old): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",tblTradeNewForOld.id)
      json.put("created_by",tblTradeNewForOld.created_by)
      json.put("created_date",tblTradeNewForOld.created_date)
      json.put("last_modified_by",tblTradeNewForOld.last_modified_by)
      json.put("last_modified_date",tblTradeNewForOld.last_modified_date)
      json.put("oldMachineBrand",tblTradeNewForOld.oldMachineBrand)
      json.put("oldMachineType",tblTradeNewForOld.oldMachineType)
      json.put("oldMachineNum",numberFormatUtil.panduanInt1(tblTradeNewForOld.oldMachineNum))
      json.put("connector",tblTradeNewForOld.connector)
      json.put("connectWay",tblTradeNewForOld.connectWay)
      json.put("hxmxid",tblTradeNewForOld.hxmxid)
      json.put("pgguid",tblTradeNewForOld.pgguid)
      json.put("xsdh",tblTradeNewForOld.xsdh)
      json.put("orderitemid",tblTradeNewForOld.orderitemid)
      json.put("lcid",tblTradeNewForOld.lcid)
      json.put("ts",tblTradeNewForOld.ts)
      json.put("table",tblTradeNewForOld.table)


    } catch {
      case e: Exception => logger.error("同步TblTradeNewForOld表出现异常" + e.getMessage)
    }
    json
  }

  def getTblAzAssignLcFzryJson(tblAzAssignLcFzry: Tbl_Az_Assign_Lc_Fzry): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",tblAzAssignLcFzry.id)
      json.put("created_by",tblAzAssignLcFzry.created_by)
      json.put("created_date",tblAzAssignLcFzry.created_date)
      json.put("last_modified_by",tblAzAssignLcFzry.last_modified_by)
      json.put("last_modified_date",tblAzAssignLcFzry.last_modified_date)
      json.put("azren",tblAzAssignLcFzry.azren)
      json.put("azrenid",numberFormatUtil.panduanInt(tblAzAssignLcFzry.azrenid))
      json.put("pgguid",tblAzAssignLcFzry.pgguid)
      json.put("ts",tblAzAssignLcFzry.ts)
      json.put("table",tblAzAssignLcFzry.table)
    } catch {
      case e: Exception => logger.error("同步TblAzAssignLcFzry表出现异常" + e.getMessage)
    }
    json
  }

  def getTblAzAssignFeeJson(tblAzAssignFee: Tbl_Az_Assign_Fee): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",tblAzAssignFee.id)
      json.put("created_by",tblAzAssignFee.created_by)
      json.put("created_date",tblAzAssignFee.created_date)
      json.put("last_modified_by",tblAzAssignFee.last_modified_by)
      json.put("last_modified_date",tblAzAssignFee.last_modified_date)
      json.put("otherfee",tblAzAssignFee.otherfee)
      json.put("totalfee",tblAzAssignFee.totalfee)
      json.put("ajia",tblAzAssignFee.ajia)
      json.put("jcguan",tblAzAssignFee.jcguan)
      json.put("kqkg",tblAzAssignFee.kqkg)
      json.put("gkzy",tblAzAssignFee.gkzy)
      json.put("yccxqk",tblAzAssignFee.yccxqk)
      json.put("flbz",tblAzAssignFee.flbz)
      json.put("pgguid",tblAzAssignFee.pgguid)
      json.put("ts",tblAzAssignFee.ts)
      json.put("table",tblAzAssignFee.table)

    } catch {
      case e: Exception => logger.error("同步TblAzAssignFee表出现异常" + e.getMessage)
    }
    json
  }

  //安装预约表
  def getTblAzAssignAppointmentJson(tbl_Az_Assign_Appointment: Tbl_Az_Assign_Appointment): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",tbl_Az_Assign_Appointment.id)
      json.put("created_by",tbl_Az_Assign_Appointment.created_by)
      json.put("created_date",tbl_Az_Assign_Appointment.created_date)
      json.put("last_modified_by",tbl_Az_Assign_Appointment.last_modified_by)
      json.put("last_modified_date",tbl_Az_Assign_Appointment.last_modified_date)
      json.put("kssj",numberFormatUtil.panduanDate(tbl_Az_Assign_Appointment.kssj))
      json.put("jssj",numberFormatUtil.panduanDate(tbl_Az_Assign_Appointment.jssj))
      json.put("czren",tbl_Az_Assign_Appointment.czren)
      json.put("pgguid",tbl_Az_Assign_Appointment.pgguid)
      json.put("czsj",numberFormatUtil.panduanDate(tbl_Az_Assign_Appointment.czsj))
      json.put("leix",tbl_Az_Assign_Appointment.leix)
      json.put("reason",tbl_Az_Assign_Appointment.reason)
      json.put("beiz",tbl_Az_Assign_Appointment.beiz)
      json.put("ts",tbl_Az_Assign_Appointment.ts)
      json.put("table",tbl_Az_Assign_Appointment.table)
    } catch {
      case e: Exception => logger.error("同步预约表出现异常" + e.getMessage)
    }
    json
  }

  //安装主表
  def getTblAzAssignLcLsJson(tbl_Az_Assign_Lc_Ls: Tbl_Az_Assign_Lc_Ls): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("pgguid",tbl_Az_Assign_Lc_Ls.pgguid)
      json.put("created_by",tbl_Az_Assign_Lc_Ls.created_by)
      json.put("created_date",tbl_Az_Assign_Lc_Ls.created_date)
      json.put("last_modified_by",tbl_Az_Assign_Lc_Ls.last_modified_by)
      json.put("last_modified_date",tbl_Az_Assign_Lc_Ls.last_modified_date)
      json.put("pgid",tbl_Az_Assign_Lc_Ls.pgid)
      json.put("yhmc",tbl_Az_Assign_Lc_Ls.yhmc)
      json.put("yddh",tbl_Az_Assign_Lc_Ls.yddh)
      json.put("yddh2",tbl_Az_Assign_Lc_Ls.yddh2)
      json.put("quhao",tbl_Az_Assign_Lc_Ls.quhao)
      json.put("dhhm",tbl_Az_Assign_Lc_Ls.dhhm)
      json.put("fjhm",tbl_Az_Assign_Lc_Ls.fjhm)
      json.put("email",tbl_Az_Assign_Lc_Ls.email)
      json.put("sfen",tbl_Az_Assign_Lc_Ls.sfen)
      json.put("cshi",tbl_Az_Assign_Lc_Ls.cshi)
      json.put("xian",tbl_Az_Assign_Lc_Ls.xian)
      json.put("xzhen",tbl_Az_Assign_Lc_Ls.xzhen)
      json.put("dizi",tbl_Az_Assign_Lc_Ls.dizi)
      json.put("xxqd",tbl_Az_Assign_Lc_Ls.xxqd)
      json.put("xxly",tbl_Az_Assign_Lc_Ls.xxly)
      json.put("xxlb",tbl_Az_Assign_Lc_Ls.xxlb)
      json.put("beiz",tbl_Az_Assign_Lc_Ls.beiz)
      json.put("yhsx",tbl_Az_Assign_Lc_Ls.yhsx)
      json.put("yxji",tbl_Az_Assign_Lc_Ls.yxji)
      json.put("gdhao",tbl_Az_Assign_Lc_Ls.gdhao)
      json.put("spid",tbl_Az_Assign_Lc_Ls.spid)
      json.put("spmc",tbl_Az_Assign_Lc_Ls.spmc)
      json.put("azren",tbl_Az_Assign_Lc_Ls.azren)
      json.put("azrenid",tbl_Az_Assign_Lc_Ls.azrenid)
      json.put("azwdxtbh",tbl_Az_Assign_Lc_Ls.azwdxtbh)
      json.put("azwdno",tbl_Az_Assign_Lc_Ls.azwdno)
      json.put("azwdmc",tbl_Az_Assign_Lc_Ls.azwdmc)
      json.put("jspgwdno",tbl_Az_Assign_Lc_Ls.jspgwdno)
      json.put("jspgwdmc",tbl_Az_Assign_Lc_Ls.jspgwdmc)
      json.put("jspgwdxtbh",tbl_Az_Assign_Lc_Ls.jspgwdxtbh)
      json.put("jspgwdsj",tbl_Az_Assign_Lc_Ls.jspgwdsj)
      json.put("zxha",tbl_Az_Assign_Lc_Ls.zxha)
      json.put("ssqy",tbl_Az_Assign_Lc_Ls.ssqy)
      json.put("qqlyno",tbl_Az_Assign_Lc_Ls.qqlyno)
      json.put("qqlymc",tbl_Az_Assign_Lc_Ls.qqlymc)
      json.put("qqlyxh",tbl_Az_Assign_Lc_Ls.qqlyxh)
      json.put("qqlyzj",tbl_Az_Assign_Lc_Ls.qqlyzj)
      json.put("bjustat",numberFormatUtil.panduanInt(tbl_Az_Assign_Lc_Ls.bjustat))
      json.put("yhqwkssj",tbl_Az_Assign_Lc_Ls.yhqwkssj)
      json.put("yhqwjssj",tbl_Az_Assign_Lc_Ls.yhqwjssj)
      json.put("stat",numberFormatUtil.panduanInt(tbl_Az_Assign_Lc_Ls.stat))
      json.put("gpsdzxx",tbl_Az_Assign_Lc_Ls.gpsdzxx)
      json.put("cjren",tbl_Az_Assign_Lc_Ls.cjren)
      json.put("cjrmc",tbl_Az_Assign_Lc_Ls.cjrmc)
      json.put("cjdt",tbl_Az_Assign_Lc_Ls.cjdt)
      json.put("cjwdno",tbl_Az_Assign_Lc_Ls.cjwdno)
      json.put("cjwdxtbh",tbl_Az_Assign_Lc_Ls.cjwdxtbh)
      json.put("zjczren",tbl_Az_Assign_Lc_Ls.zjczren)
      json.put("zjczwd",tbl_Az_Assign_Lc_Ls.zjczwd)
      json.put("zjczwdxtbh",tbl_Az_Assign_Lc_Ls.zjczwdxtbh)
      json.put("zjczsj",tbl_Az_Assign_Lc_Ls.zjczsj)
      json.put("xslx",tbl_Az_Assign_Lc_Ls.xslx)
      json.put("lcid",tbl_Az_Assign_Lc_Ls.lcid)
      json.put("djlxno",tbl_Az_Assign_Lc_Ls.djlxno)
      json.put("yyazsj",tbl_Az_Assign_Lc_Ls.yyazsj)
      json.put("sfwcps",tbl_Az_Assign_Lc_Ls.sfwcps)
      json.put("xsdh",tbl_Az_Assign_Lc_Ls.xsdh)
      json.put("gcbh",tbl_Az_Assign_Lc_Ls.gcbh)
      json.put("gcmc",tbl_Az_Assign_Lc_Ls.gcmc)
      json.put("azsl",numberFormatUtil.panduanInt(tbl_Az_Assign_Lc_Ls.azsl))
      json.put("wwsl",numberFormatUtil.panduanInt(tbl_Az_Assign_Lc_Ls.wwsl))
      json.put("lxren",tbl_Az_Assign_Lc_Ls.lxren)
      json.put("xsdwno",tbl_Az_Assign_Lc_Ls.xsdwno)
      json.put("xswdmc",tbl_Az_Assign_Lc_Ls.xswdmc)
      json.put("xsdwxtbh",tbl_Az_Assign_Lc_Ls.xsdwxtbh)
      json.put("fphm",tbl_Az_Assign_Lc_Ls.fphm)
      json.put("gmsj",tbl_Az_Assign_Lc_Ls.gmsj)
      json.put("kqbh",tbl_Az_Assign_Lc_Ls.kqbh)
      json.put("xsorsh",numberFormatUtil.panduanInt1(tbl_Az_Assign_Lc_Ls.xsorsh))
      json.put("dqjdsj",tbl_Az_Assign_Lc_Ls.dqjdsj)
      json.put("syjd",tbl_Az_Assign_Lc_Ls.syjd)
      json.put("dqjd",tbl_Az_Assign_Lc_Ls.dqjd)
      json.put("jindu",tbl_Az_Assign_Lc_Ls.jindu)
      json.put("weidu",tbl_Az_Assign_Lc_Ls.weidu)
      json.put("shsj",tbl_Az_Assign_Lc_Ls.shsj)
      json.put("sfygllc",numberFormatUtil.panduanInt(tbl_Az_Assign_Lc_Ls.sfygllc))
      json.put("xslxid",tbl_Az_Assign_Lc_Ls.xslxid)
      json.put("yhsxid",tbl_Az_Assign_Lc_Ls.yhsxid)
      json.put("xxlbid",tbl_Az_Assign_Lc_Ls.xxlbid)
      json.put("xxlyid",tbl_Az_Assign_Lc_Ls.xxlyid)
      json.put("sfenid",tbl_Az_Assign_Lc_Ls.sfenid)
      json.put("cshiid",tbl_Az_Assign_Lc_Ls.cshiid)
      json.put("xianid",tbl_Az_Assign_Lc_Ls.xianid)
      json.put("xzhenid",tbl_Az_Assign_Lc_Ls.xzhenid)
      json.put("wcsj",numberFormatUtil.panduanDate(tbl_Az_Assign_Lc_Ls.wcsj))
      json.put("retailsign",numberFormatUtil.panduanInt1(tbl_Az_Assign_Lc_Ls.retailsign))
      json.put("servicewdmc",tbl_Az_Assign_Lc_Ls.servicewdmc)
      json.put("shopname",tbl_Az_Assign_Lc_Ls.shopname)
      json.put("orderphone",tbl_Az_Assign_Lc_Ls.orderphone)
      json.put("extendfiled1",tbl_Az_Assign_Lc_Ls.extendfiled1)
      json.put("extendfiled2",tbl_Az_Assign_Lc_Ls.extendfiled2)
      json.put("extendfiled3",tbl_Az_Assign_Lc_Ls.extendfiled3)
      json.put("extendfiled4",tbl_Az_Assign_Lc_Ls.extendfiled4)
      json.put("servicewdno",tbl_Az_Assign_Lc_Ls.servicewdno)
      json.put("shopno",tbl_Az_Assign_Lc_Ls.shopno)
      json.put("extendfiled5",tbl_Az_Assign_Lc_Ls.extendfiled5)
      json.put("organizationaddress",tbl_Az_Assign_Lc_Ls.organizationaddress)
      json.put("organizationname",tbl_Az_Assign_Lc_Ls.organizationname)
      json.put("ts",tbl_Az_Assign_Lc_Ls.ts)
      json.put("table",tbl_Az_Assign_Lc_Ls.table)

    } catch {
      case e: Exception => logger.error("同步主表出现异常" + e.getMessage)
    }
    json
  }

  //安装反馈明细
  def getTblAzAssignFkmxJson(tbl_Az_Assign_Fkmx: Tbl_Az_Assign_Fkmx): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("fkid",tbl_Az_Assign_Fkmx.fkid)
      json.put("created_by",tbl_Az_Assign_Fkmx.created_by)
      json.put("created_date",tbl_Az_Assign_Fkmx.created_date)
      json.put("last_modified_by",tbl_Az_Assign_Fkmx.last_modified_by)
      json.put("last_modified_date",tbl_Az_Assign_Fkmx.last_modified_date)
      json.put("fklb",tbl_Az_Assign_Fkmx.fklb)
      json.put("fkjg",tbl_Az_Assign_Fkmx.fkjg)
      json.put("fknr",tbl_Az_Assign_Fkmx.fknr)
      json.put("fkren",tbl_Az_Assign_Fkmx.fkren)
      json.put("fkrenmc",tbl_Az_Assign_Fkmx.fkrenmc)
      json.put("fksj",numberFormatUtil.panduanDate(tbl_Az_Assign_Fkmx.fksj))
      json.put("xtwdbh",tbl_Az_Assign_Fkmx.xtwdbh)
      json.put("wdno",tbl_Az_Assign_Fkmx.wdno)
      json.put("wdmc",tbl_Az_Assign_Fkmx.wdmc)
      json.put("pgguid",tbl_Az_Assign_Fkmx.pgguid)
      json.put("cjdt",numberFormatUtil.panduanDate(tbl_Az_Assign_Fkmx.cjdt))
      json.put("ts",tbl_Az_Assign_Fkmx.ts)
      json.put("table",tbl_Az_Assign_Fkmx.table)
    } catch {
      case e: Exception => logger.error("同步反馈明细表出现异常" + e.getMessage)
    }
    json
  }

  //安装工单明细
  def getTblAzAssignMx(tblAzAssignMx: Tbl_Az_Assign_Mx): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]()

    try {
      json.put("pgmxid", tblAzAssignMx.pgmxid)
      json.put("created_by", tblAzAssignMx.created_by)
      json.put("created_date", tblAzAssignMx.created_date)
      json.put("last_modified_by", tblAzAssignMx.last_modified_by)
      json.put("last_modified_date", tblAzAssignMx.last_modified_date)
      json.put("pgguid", tblAzAssignMx.pgguid)
      json.put("spid", tblAzAssignMx.spid)
      json.put("spmc", tblAzAssignMx.spmc)
      json.put("xlid", tblAzAssignMx.xlid)
      json.put("xlmc", tblAzAssignMx.xlmc)
      json.put("xiid", tblAzAssignMx.xiid)
      json.put("ximc", tblAzAssignMx.ximc)
      json.put("jxmc", tblAzAssignMx.jxmc)
      json.put("jxno", tblAzAssignMx.jxno)
      json.put("czren", tblAzAssignMx.czren)
      json.put("czsj", numberFormatUtil.panduanDate(tblAzAssignMx.czsj))
      json.put("czwd", tblAzAssignMx.czwd)
      json.put("njtm", tblAzAssignMx.njtm)
      json.put("wjtm", tblAzAssignMx.wjtm)
      json.put("beiz", tblAzAssignMx.beiz)
      json.put("shul", numberFormatUtil.panduanInt(tblAzAssignMx.shul))
      json.put("cjdt", numberFormatUtil.panduanDate(tblAzAssignMx.cjdt))
      json.put("jiage", numberFormatUtil.panduanDouble(tblAzAssignMx.jiage))
      json.put("danw", tblAzAssignMx.danw)
      json.put("wldm", tblAzAssignMx.wldm)
      json.put("njtm2", numberFormatUtil.panduanInt(tblAzAssignMx.njtm2))
      json.put("wjsl", numberFormatUtil.panduanInt1(tblAzAssignMx.wjsl))
      json.put("njsl", numberFormatUtil.panduanInt1(tblAzAssignMx.njsl))
      json.put("wwsl", numberFormatUtil.panduanInt(tblAzAssignMx.wwsl))
      json.put("ts",tblAzAssignMx.ts)
      json.put("table",tblAzAssignMx.table)

    } catch {
      case e: Exception => logger.error("同步安装明细表出现异常" + e.getMessage)
    }
    json
  }

  //安装客户评价
  def getTblAzAssignSatisfaction(tblAzAssignSatisfaction: Tbl_Az_Assign_Satisfaction): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]()
    try {
      json.put("id", tblAzAssignSatisfaction.id)
      json.put("created_by", tblAzAssignSatisfaction.created_by)
      json.put("created_date", tblAzAssignSatisfaction.created_date)
      json.put("last_modified_by", tblAzAssignSatisfaction.last_modified_by)
      json.put("last_modified_date", tblAzAssignSatisfaction.last_modified_date)
      json.put("pgguid", tblAzAssignSatisfaction.pgguid)
      json.put("pjly", tblAzAssignSatisfaction.pjly)
      json.put("pjnr", tblAzAssignSatisfaction.pjnr)
      json.put("hfren", tblAzAssignSatisfaction.hfren)
      json.put("hfwdmc", tblAzAssignSatisfaction.hfwdmc)
      json.put("hfwdno", tblAzAssignSatisfaction.hfwdno)
      json.put("hfsj", numberFormatUtil.panduanDate(tblAzAssignSatisfaction.hfsj))
      json.put("bmylx", tblAzAssignSatisfaction.bmylx)
      json.put("bmybeiz", tblAzAssignSatisfaction.bmybeiz)
      json.put("bmysj", numberFormatUtil.panduanDate(tblAzAssignSatisfaction.bmysj))
      json.put("splb", numberFormatUtil.panduanLong(tblAzAssignSatisfaction.splb))
      json.put("mydlx", numberFormatUtil.panduanInt(tblAzAssignSatisfaction.mydlx))
      json.put("sxlx", numberFormatUtil.panduanInt(tblAzAssignSatisfaction.sxlx))
      json.put("ts",tblAzAssignSatisfaction.ts)
      json.put("table",tblAzAssignSatisfaction.table)
    } catch {
      case e: Exception => logger.error("同步安装客户评价表出现异常" + e.getMessage)
    }
    json
  }

  //安装配送表
  def getTblAzAssignPslclsJson(tbl_Az_Assign_Pslc_Ls: Tbl_Az_Assign_Pslc_Ls): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("pgguid",tbl_Az_Assign_Pslc_Ls.pgguid)
      json.put("created_by",tbl_Az_Assign_Pslc_Ls.created_by)
      json.put("created_date",tbl_Az_Assign_Pslc_Ls.created_date)
      json.put("last_modified_by",tbl_Az_Assign_Pslc_Ls.last_modified_by)
      json.put("last_modified_date",tbl_Az_Assign_Pslc_Ls.last_modified_date)
      json.put("azren",tbl_Az_Assign_Pslc_Ls.azren)
      json.put("azrenid",tbl_Az_Assign_Pslc_Ls.azrenid)
      json.put("azsl",tbl_Az_Assign_Pslc_Ls.azsl)
      json.put("azwdmc",tbl_Az_Assign_Pslc_Ls.azwdmc)
      json.put("azwdno",tbl_Az_Assign_Pslc_Ls.azwdno)
      json.put("azwdxtbh",tbl_Az_Assign_Pslc_Ls.azwdxtbh)
      json.put("beiz",tbl_Az_Assign_Pslc_Ls. beiz)
      json.put("bjustat",tbl_Az_Assign_Pslc_Ls.bjustat)
      json.put("cjdt",tbl_Az_Assign_Pslc_Ls.cjdt)
      json.put("cjren",tbl_Az_Assign_Pslc_Ls.cjren)
      json.put("cjrmc",tbl_Az_Assign_Pslc_Ls.cjrmc)
      json.put("cjwdno",tbl_Az_Assign_Pslc_Ls.cjwdno)
      json.put("cjwdxtbh",tbl_Az_Assign_Pslc_Ls.cjwdxtbh)
      json.put("cshi",tbl_Az_Assign_Pslc_Ls. cshi)
      json.put("dhhm",tbl_Az_Assign_Pslc_Ls. dhhm)
      json.put("dizi",tbl_Az_Assign_Pslc_Ls.dizi)
      json.put("djlxno",tbl_Az_Assign_Pslc_Ls.djlxno)
      json.put("dqjd",tbl_Az_Assign_Pslc_Ls.dqjd)
      json.put("dqjdsj",tbl_Az_Assign_Pslc_Ls.dqjdsj)
      json.put("email",tbl_Az_Assign_Pslc_Ls.email)
      json.put("fjhm",tbl_Az_Assign_Pslc_Ls.fjhm)
      json.put("fphm",tbl_Az_Assign_Pslc_Ls.fphm)
      json.put("gcbh",tbl_Az_Assign_Pslc_Ls.gcbh)
      json.put("gcmc",tbl_Az_Assign_Pslc_Ls.gcmc)
      json.put("gdhao",tbl_Az_Assign_Pslc_Ls.gdhao)
      json.put("gmsj",tbl_Az_Assign_Pslc_Ls.gmsj)
      json.put("gpsdzxx",tbl_Az_Assign_Pslc_Ls.gpsdzxx)
      json.put("jindu",tbl_Az_Assign_Pslc_Ls.jindu)
      json.put("jspgwdmc",tbl_Az_Assign_Pslc_Ls.jspgwdmc)
      json.put("jspgwdno",tbl_Az_Assign_Pslc_Ls.jspgwdno)
      json.put("jspgwdsj",tbl_Az_Assign_Pslc_Ls.jspgwdsj)
      json.put("jspgwdxtbh",tbl_Az_Assign_Pslc_Ls.jspgwdxtbh)
      json.put("kqbh",tbl_Az_Assign_Pslc_Ls.kqbh)
      json.put("lcid",tbl_Az_Assign_Pslc_Ls.lcid)
      json.put("lxren",tbl_Az_Assign_Pslc_Ls.lxren)
      json.put("pgid",tbl_Az_Assign_Pslc_Ls.pgid)
      json.put("qqlymc",tbl_Az_Assign_Pslc_Ls.qqlymc)
      json.put("qqlyno",tbl_Az_Assign_Pslc_Ls.qqlyno)
      json.put("qqlyxh",tbl_Az_Assign_Pslc_Ls.qqlyxh)
      json.put("qqlyzj",tbl_Az_Assign_Pslc_Ls.qqlyzj)
      json.put("quhao",tbl_Az_Assign_Pslc_Ls.quhao)
      json.put("sfen",tbl_Az_Assign_Pslc_Ls.sfen)
      json.put("sfwcps",tbl_Az_Assign_Pslc_Ls. sfwcps)
      json.put("sfygllc",tbl_Az_Assign_Pslc_Ls. sfygllc)
      json.put("shsj",tbl_Az_Assign_Pslc_Ls. shsj)
      json.put("spid",tbl_Az_Assign_Pslc_Ls.spid)
      json.put("spmc",tbl_Az_Assign_Pslc_Ls.spmc)
      json.put("ssqy",tbl_Az_Assign_Pslc_Ls.ssqy)
      json.put("stat",tbl_Az_Assign_Pslc_Ls.stat)
      json.put("syjd",tbl_Az_Assign_Pslc_Ls.syjd)
      json.put("weidu",tbl_Az_Assign_Pslc_Ls.weidu)
      json.put("wwsl",tbl_Az_Assign_Pslc_Ls.wwsl)
      json.put("xian",tbl_Az_Assign_Pslc_Ls.xian)
      json.put("xsdh",tbl_Az_Assign_Pslc_Ls.xsdh)
      json.put("xsdwno",tbl_Az_Assign_Pslc_Ls.xsdwno)
      json.put("xsdwxtbh",tbl_Az_Assign_Pslc_Ls.xsdwxtbh)
      json.put("xslx",tbl_Az_Assign_Pslc_Ls.xslx)
      json.put("xsorsh",tbl_Az_Assign_Pslc_Ls.xsorsh)
      json.put("xswdmc",tbl_Az_Assign_Pslc_Ls.xswdmc)
      json.put("xxlb",tbl_Az_Assign_Pslc_Ls.xxlb)
      json.put("xxly",tbl_Az_Assign_Pslc_Ls.xxly)
      json.put("xxqd",tbl_Az_Assign_Pslc_Ls.xxqd)
      json.put("xzhen",tbl_Az_Assign_Pslc_Ls.xzhen)
      json.put("yddh",tbl_Az_Assign_Pslc_Ls.yddh)
      json.put("yddh2",tbl_Az_Assign_Pslc_Ls.yddh2)
      json.put("yhmc",tbl_Az_Assign_Pslc_Ls.yhmc)
      json.put("yhqwjssj",tbl_Az_Assign_Pslc_Ls.yhqwjssj)
      json.put("yhqwkssj",tbl_Az_Assign_Pslc_Ls.yhqwkssj)
      json.put("yxji",tbl_Az_Assign_Pslc_Ls.yxji)
      json.put("yyazsj",tbl_Az_Assign_Pslc_Ls.yyazsj)
      json.put("zjczren",tbl_Az_Assign_Pslc_Ls.zjczren)
      json.put("zjczsj",tbl_Az_Assign_Pslc_Ls.zjczsj)
      json.put("zjczwd",tbl_Az_Assign_Pslc_Ls.zjczwd)
      json.put("zjczwdxtbh",tbl_Az_Assign_Pslc_Ls.zjczwdxtbh)
      json.put("zxha",tbl_Az_Assign_Pslc_Ls.zxha)
      json.put("cshiid",tbl_Az_Assign_Pslc_Ls.cshiid)
      json.put("xzhenid",tbl_Az_Assign_Pslc_Ls.xzhenid)
      json.put("sfenid",tbl_Az_Assign_Pslc_Ls.sfenid)
      json.put("xianid",tbl_Az_Assign_Pslc_Ls.xianid)
      json.put("xxlyid",tbl_Az_Assign_Pslc_Ls.xxlyid)
      json.put("xslxid",tbl_Az_Assign_Pslc_Ls.xslxid)
      json.put("yhsxid",tbl_Az_Assign_Pslc_Ls.yhsxid)
      json.put("organizationname",tbl_Az_Assign_Pslc_Ls.organizationname)
      json.put("organizationaddress",tbl_Az_Assign_Pslc_Ls.organizationaddress)
      json.put("name",tbl_Az_Assign_Pslc_Ls.name)
      json.put("mobile",tbl_Az_Assign_Pslc_Ls.mobile)
      json.put("retailsign",tbl_Az_Assign_Pslc_Ls.retailsign)
      json.put("appointmentkssj",tbl_Az_Assign_Pslc_Ls.appointmentkssj)
      json.put("appointmentjssj",tbl_Az_Assign_Pslc_Ls.appointmentjssj)
      json.put("zjczwdxtbh",tbl_Az_Assign_Pslc_Ls.zjczwdxtbh)
      json.put("wcsj",tbl_Az_Assign_Pslc_Ls.wcsj)
      json.put("requisition",tbl_Az_Assign_Pslc_Ls.requisition)
      json.put("shopno",tbl_Az_Assign_Pslc_Ls.shopno)
      json.put("shopname",tbl_Az_Assign_Pslc_Ls.shopname)
      json.put("ts",tbl_Az_Assign_Pslc_Ls.ts)
      json.put("table",tbl_Az_Assign_Pslc_Ls.table)
    } catch {
      case e: Exception => logger.error("同步到AzAssignPslcls表异常" + e.getMessage)
    }
    json
  }



}