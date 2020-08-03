package com.gree.util

import java.util

import com.gree.model.{TblWangdianSjdwmx, WangdianLevel}
import org.slf4j.{Logger, LoggerFactory}


object JsonBeanUtil {
  private val numberFormatUtil = new NumberFormatUtil()
  private val logger: Logger = LoggerFactory.getLogger(JsonBeanUtil.getClass)


  def getTblWangdianSjdwmxJson(tblWangdianSjdwmx: TblWangdianSjdwmx): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",tblWangdianSjdwmx.id)
      json.put("created_by",tblWangdianSjdwmx.created_by)
      json.put("created_date",numberFormatUtil.panduanDate(tblWangdianSjdwmx.created_date))
      json.put("last_modified_by",tblWangdianSjdwmx.last_modified_by)
      json.put("last_modified_date",numberFormatUtil.panduanDate(tblWangdianSjdwmx.last_modified_date))
      json.put("xhid",numberFormatUtil.panduanLong(tblWangdianSjdwmx.xhid))
      json.put("xtwdbh",tblWangdianSjdwmx.xtwdbh)
      json.put("wdno",tblWangdianSjdwmx.wdno)
      json.put("splb",numberFormatUtil.panduanInt(tblWangdianSjdwmx.splb))
      json.put("scqy",tblWangdianSjdwmx.scqy)
      json.put("fwlb",tblWangdianSjdwmx.fwlb)
      json.put("sjwdno",tblWangdianSjdwmx.sjwdno)
      json.put("sjwdmc",tblWangdianSjdwmx.sjwdmc)
      json.put("sjwdxtbh",tblWangdianSjdwmx.sjwdxtbh)
      json.put("cxqyfw",tblWangdianSjdwmx.cxqyfw)
      json.put("stat",numberFormatUtil.panduanInt(tblWangdianSjdwmx.stat))
      json.put("czren",tblWangdianSjdwmx.czren)
      json.put("czrmc",tblWangdianSjdwmx.czrmc)
      json.put("czsj",numberFormatUtil.panduanDate(tblWangdianSjdwmx.czsj))
      json.put("zhczsj",numberFormatUtil.panduanDate(tblWangdianSjdwmx.zhczsj))
      json.put("cxqyfwbh",tblWangdianSjdwmx.cxqyfwbh)
      json.put("sfxyzq",tblWangdianSjdwmx.sfxyzq)
      json.put("sfxyzqbak",tblWangdianSjdwmx.sfxyzqbak)
      json.put("seno",numberFormatUtil.panduanLong(tblWangdianSjdwmx.seno))
      json.put("leix",numberFormatUtil.panduanLong(tblWangdianSjdwmx.leix))
      json.put("wdmc",tblWangdianSjdwmx.wdmc)
      json.put("spmc",tblWangdianSjdwmx.spmc)
      json.put("scqymc",tblWangdianSjdwmx.scqymc)
    } catch {
      case e: Exception => logger.error("同步TblWangdianSjdwmx表出现异常" + e.getMessage)
    }
    json
  }


  def getWangdianLevelJson(wangdianLevel: WangdianLevel): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]
    try {
      json.put("id",wangdianLevel.id)
      json.put("created_by",wangdianLevel.created_by)
      json.put("created_date",numberFormatUtil.panduanDate(wangdianLevel.created_date))
      json.put("last_modified_by",wangdianLevel.last_modified_by)
      json.put("last_modified_date",numberFormatUtil.panduanDate(wangdianLevel.last_modified_date))
      json.put("xhid",numberFormatUtil.panduanLong(wangdianLevel.xhid))
      json.put("xtwdbh",wangdianLevel.xtwdbh)
      json.put("wdno",wangdianLevel.wdno)
      json.put("splb",numberFormatUtil.panduanInt(wangdianLevel.splb))
      json.put("scqy",wangdianLevel.scqy)
      json.put("fwlb",wangdianLevel.fwlb)
      json.put("sjwdno",wangdianLevel.sjwdno)
      json.put("sjwdmc",wangdianLevel.sjwdmc)
      json.put("sjwdxtbh",wangdianLevel.sjwdxtbh)
      json.put("cxqyfw",wangdianLevel.cxqyfw)
      json.put("stat",numberFormatUtil.panduanInt(wangdianLevel.stat))
      json.put("czren",wangdianLevel.czren)
      json.put("czrmc",wangdianLevel.czrmc)
      json.put("czsj",numberFormatUtil.panduanDate(wangdianLevel.czsj))
      json.put("zhczsj",numberFormatUtil.panduanDate(wangdianLevel.zhczsj))
      json.put("cxqyfwbh",wangdianLevel.cxqyfwbh)
      json.put("sfxyzq",wangdianLevel.sfxyzq)
      json.put("sfxyzqbak",wangdianLevel.sfxyzqbak)
      json.put("seno",numberFormatUtil.panduanLong(wangdianLevel.seno))
      json.put("leix",numberFormatUtil.panduanLong(wangdianLevel.leix))
      json.put("wdmc",wangdianLevel.wdmc)
      json.put("spmc",wangdianLevel.spmc)
      json.put("scqymc",wangdianLevel.scqymc)
      json.put("first",wangdianLevel.first)
      json.put("second",wangdianLevel.second)
      json.put("third",wangdianLevel.third)
      json.put("fourth",wangdianLevel.fourth)
      json.put("fifth",wangdianLevel.fifth)
      json.put("sixth",wangdianLevel.sixth)
      json.put("seventh",wangdianLevel.seventh)
    } catch {
      case e: Exception => logger.error("同步WangdianLevel表出现异常" + e.getMessage)
    }
    json
  }

}
