package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Pslc_Ls
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Az_Assign_Pslc_Ls_ES(tblAzAssignPslcLs: OutputTag[Tbl_Az_Assign_Pslc_Ls]) extends ProcessFunction[ObjectNode, Tbl_Az_Assign_Pslc_Ls] {
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Pslc_Ls]#Context, out: Collector[Tbl_Az_Assign_Pslc_Ls]): Unit = {

    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Pslc_Ls_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()


    if ("DELETE".equals(caoZuoType)) {
      logger.info("AzAssignPslcLsFunction此数据为DELETE不做处理")
    } else {
      while (data.hasNext) {
        val azAssignPslcLs: JsonNode = data.next()
        //需要同步的数据
        try {

          out.collect(Tbl_Az_Assign_Pslc_Ls(
            azAssignPslcLs.get("pgguid").asText(),
            azAssignPslcLs.get("created_by").asText(),//tbl_Az_Assign_Pslc_Ls.created_by)
            azAssignPslcLs.get("created_date").asText(),//tbl_Az_Assign_Pslc_Ls.created_date)
            azAssignPslcLs.get("last_modified_by").asText(),//tbl_Az_Assign_Pslc_Ls.last_modified_by)
            azAssignPslcLs.get("last_modified_date").asText(),//tbl_Az_Assign_Pslc_Ls.last_modified_date)
            azAssignPslcLs.get("azren").asText(),//tbl_Az_Assign_Pslc_Ls.azren)
            azAssignPslcLs.get("azrenid").asText(),//tbl_Az_Assign_Pslc_Ls.azrenid)
            azAssignPslcLs.get("azsl").asText(),//tbl_Az_Assign_Pslc_Ls.azsl)
            azAssignPslcLs.get("azwdmc").asText(),//tbl_Az_Assign_Pslc_Ls.azwdmc)
            azAssignPslcLs.get("azwdno").asText(),//tbl_Az_Assign_Pslc_Ls.azwdno)
            azAssignPslcLs.get("azwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.azwdxtbh)
            azAssignPslcLs.get("beiz").asText(),//tbl_Az_Assign_Pslc_Ls. beiz)
            azAssignPslcLs.get("bjustat").asText(),//tbl_Az_Assign_Pslc_Ls.bjustat)
            azAssignPslcLs.get("cjdt").asText(),//tbl_Az_Assign_Pslc_Ls.cjdt)
            azAssignPslcLs.get("cjren").asText(),//tbl_Az_Assign_Pslc_Ls.cjren)
            azAssignPslcLs.get("cjrmc").asText(),//tbl_Az_Assign_Pslc_Ls.cjrmc)
            azAssignPslcLs.get("cjwdno").asText(),//tbl_Az_Assign_Pslc_Ls.cjwdno)
            azAssignPslcLs.get("cjwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.cjwdxtbh)
            azAssignPslcLs.get("cshi").asText(),//tbl_Az_Assign_Pslc_Ls. cshi)
            azAssignPslcLs.get("dhhm").asText(),//tbl_Az_Assign_Pslc_Ls. dhhm)
            azAssignPslcLs.get("dizi").asText(),//tbl_Az_Assign_Pslc_Ls.dizi)
            azAssignPslcLs.get("djlxno").asText(),//tbl_Az_Assign_Pslc_Ls.djlxno)
            azAssignPslcLs.get("dqjd").asText(),//tbl_Az_Assign_Pslc_Ls.dqjd)
            azAssignPslcLs.get("dqjdsj").asText(),//tbl_Az_Assign_Pslc_Ls.dqjd)
            azAssignPslcLs.get("email").asText(),//tbl_Az_Assign_Pslc_Ls.email)
            azAssignPslcLs.get("fjhm").asText(),//tbl_Az_Assign_Pslc_Ls.fjhm)
            azAssignPslcLs.get("fphm").asText(),//tbl_Az_Assign_Pslc_Ls.fphm	)
            azAssignPslcLs.get("gcbh").asText(),//tbl_Az_Assign_Pslc_Ls.gcbh	)
            azAssignPslcLs.get("gcmc").asText(),//tbl_Az_Assign_Pslc_Ls.gcmc	)
            azAssignPslcLs.get("gdhao").asText(),//tbl_Az_Assign_Pslc_Ls.gdhao)
            azAssignPslcLs.get("gmsj").asText(),//tbl_Az_Assign_Pslc_Ls.gmsj)
            azAssignPslcLs.get("gpsdzxx").asText(),//tbl_Az_Assign_Pslc_Ls.gpsdzxx)
            azAssignPslcLs.get("jindu").asText(),//tbl_Az_Assign_Pslc_Ls.jindu)
            azAssignPslcLs.get("jspgwdmc").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdmc)
            azAssignPslcLs.get("jspgwdno").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdno)
            azAssignPslcLs.get("jspgwdsj").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdsj)
            azAssignPslcLs.get("jspgwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdxtbh)
            azAssignPslcLs.get("kqbh").asText(),//tbl_Az_Assign_Pslc_Ls.kqbh)
            azAssignPslcLs.get("lcid").asText(),//tbl_Az_Assign_Pslc_Ls.lcid)
            azAssignPslcLs.get("lxren").asText(),//tbl_Az_Assign_Pslc_Ls.lxren)
            azAssignPslcLs.get("pgid").asText(),//tbl_Az_Assign_Pslc_Ls.pgid)
            azAssignPslcLs.get("qqlymc").asText(),//tbl_Az_Assign_Pslc_Ls.qqlymc)
            azAssignPslcLs.get("qqlyno").asText(),//tbl_Az_Assign_Pslc_Ls.qqlyno)
            azAssignPslcLs.get("qqlyxh").asText(),//tbl_Az_Assign_Pslc_Ls.qqlyxh	)
            azAssignPslcLs.get("qqlyzj").asText(),//tbl_Az_Assign_Pslc_Ls.qqlyzj)
            azAssignPslcLs.get("quhao").asText(),//tbl_Az_Assign_Pslc_Ls.quhao)
            azAssignPslcLs.get("sfen").asText(),//tbl_Az_Assign_Pslc_Ls.sfen)
            azAssignPslcLs.get("sfwcps").asText(),//tbl_Az_Assign_Pslc_Ls. sfwcps)
            azAssignPslcLs.get("sfygllc").asText(),//tbl_Az_Assign_Pslc_Ls. sfygllc)
            azAssignPslcLs.get("shsj").asText(),//tbl_Az_Assign_Pslc_Ls. shsj)
            azAssignPslcLs.get("spid").asText(),//tbl_Az_Assign_Pslc_Ls.spid)
            azAssignPslcLs.get("spmc").asText(),//tbl_Az_Assign_Pslc_Ls.spmc)
            azAssignPslcLs.get("ssqy").asText(),//tbl_Az_Assign_Pslc_Ls.ssqy)
            azAssignPslcLs.get("stat").asText(),//tbl_Az_Assign_Pslc_Ls.stat)
            azAssignPslcLs.get("syjd").asText(),//tbl_Az_Assign_Pslc_Ls.syjd	)
            azAssignPslcLs.get("weidu").asText(),//tbl_Az_Assign_Pslc_Ls.weidu	)
            azAssignPslcLs.get("wwsl").asText(),//tbl_Az_Assign_Pslc_Ls.wwsl	)
            azAssignPslcLs.get("xian").asText(),//tbl_Az_Assign_Pslc_Ls.xian)
            azAssignPslcLs.get("xsdh").asText(),//tbl_Az_Assign_Pslc_Ls.xsdh	)
            azAssignPslcLs.get("xsdwno").asText(),//tbl_Az_Assign_Pslc_Ls.xsdwno)
            azAssignPslcLs.get("xsdwxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.xsdwxtbh)
            azAssignPslcLs.get("xslx").asText(),//tbl_Az_Assign_Pslc_Ls.xslx	)
            azAssignPslcLs.get("xsorsh").asText(),//tbl_Az_Assign_Pslc_Ls.xsorsh	)
            azAssignPslcLs.get("xswdmc").asText(),//tbl_Az_Assign_Pslc_Ls.xswdmc)
            azAssignPslcLs.get("xxlb").asText(),//tbl_Az_Assign_Pslc_Ls.xxlb	)
            azAssignPslcLs.get("xxly").asText(),//tbl_Az_Assign_Pslc_Ls.xxly)
            azAssignPslcLs.get("xxqd").asText(),//tbl_Az_Assign_Pslc_Ls.xxqd	)
            azAssignPslcLs.get("xzhen").asText(),//tbl_Az_Assign_Pslc_Ls.xzhen	)
            azAssignPslcLs.get("yddh").asText(),//tbl_Az_Assign_Pslc_Ls.yddh	)
            azAssignPslcLs.get("yddh2").asText(),//tbl_Az_Assign_Pslc_Ls.yddh2		)
            azAssignPslcLs.get("yhmc").asText(),//tbl_Az_Assign_Pslc_Ls.yhmc		)
            azAssignPslcLs.get("yhqwjssj").asText(),//tbl_Az_Assign_Pslc_Ls.yhqwjssj	)
            azAssignPslcLs.get("yhqwkssj").asText(),//tbl_Az_Assign_Pslc_Ls.yhqwkssj	)
            azAssignPslcLs.get("yhsx").asText(),//tbl_Az_Assign_Pslc_Ls.yhqwkssj	)
            azAssignPslcLs.get("yxji").asText(),//tbl_Az_Assign_Pslc_Ls.yxji		)
            azAssignPslcLs.get("yyazsj").asText(),//tbl_Az_Assign_Pslc_Ls.yyazsj	)
            azAssignPslcLs.get("zjczren").asText(),//tbl_Az_Assign_Pslc_Ls.zjczren	)
            azAssignPslcLs.get("zjczsj").asText(),//tbl_Az_Assign_Pslc_Ls.zjczsj		)
            azAssignPslcLs.get("zjczwd").asText(),//tbl_Az_Assign_Pslc_Ls.zjczwd	)
            azAssignPslcLs.get("zjczwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.zjczwdxtbh	)
            azAssignPslcLs.get("zxha").asText(),//tbl_Az_Assign_Pslc_Ls.zxha		)
            azAssignPslcLs.get("cshiid").asText(),//tbl_Az_Assign_Pslc_Ls.cshiid	)
            azAssignPslcLs.get("xzhenid").asText(),//tbl_Az_Assign_Pslc_Ls.xzhenid	)
            azAssignPslcLs.get("sfenid").asText(),//tbl_Az_Assign_Pslc_Ls.sfenid	)
            azAssignPslcLs.get("xianid").asText(),//tbl_Az_Assign_Pslc_Ls.xianid	)
            azAssignPslcLs.get("xxlyid").asText(),//tbl_Az_Assign_Pslc_Ls.xxlyid	)
            azAssignPslcLs.get("xslxid").asText(),//tbl_Az_Assign_Pslc_Ls.xslxid		)
            azAssignPslcLs.get("yhsxid").asText(),//tbl_Az_Assign_Pslc_Ls.yhsxid			)
            azAssignPslcLs.get("organizationname").asText(),//tbl_Az_Assign_Pslc_Ls.organizationname		)
            azAssignPslcLs.get("organizationaddress").asText(),//tbl_Az_Assign_Pslc_Ls.organizationaddress	)
            azAssignPslcLs.get("name").asText(),//tbl_Az_Assign_Pslc_Ls.name		)
            azAssignPslcLs.get("mobile").asText(),//tbl_Az_Assign_Pslc_Ls.mobile	)
            azAssignPslcLs.get("retailsign").asText(),//tbl_Az_Assign_Pslc_Ls.retailsign	)
            azAssignPslcLs.get("appointmentkssj").asText(),//tbl_Az_Assign_Pslc_Ls.appointmentkssj		)
            azAssignPslcLs.get("appointmentjssj").asText(),//tbl_Az_Assign_Pslc_Ls.appointmentjssj	)
            azAssignPslcLs.get("wcsj").asText(),
            azAssignPslcLs.get("requisition").asText(),
            azAssignPslcLs.get("shopno").asText(),
            azAssignPslcLs.get("shopname").asText(),
            ts,
            table
          ))

          //需要进行逻辑计算的数据
          ctx.output[Tbl_Az_Assign_Pslc_Ls](tblAzAssignPslcLs, Tbl_Az_Assign_Pslc_Ls(
            azAssignPslcLs.get("pgguid").asText(),
            azAssignPslcLs.get("created_by").asText(),//tbl_Az_Assign_Pslc_Ls.created_by)
            azAssignPslcLs.get("created_date").asText(),//tbl_Az_Assign_Pslc_Ls.created_date)
            azAssignPslcLs.get("last_modified_by").asText(),//tbl_Az_Assign_Pslc_Ls.last_modified_by)
            azAssignPslcLs.get("last_modified_date").asText(),//tbl_Az_Assign_Pslc_Ls.last_modified_date)
            azAssignPslcLs.get("azren").asText(),//tbl_Az_Assign_Pslc_Ls.azren)
            azAssignPslcLs.get("azrenid").asText(),//tbl_Az_Assign_Pslc_Ls.azrenid)
            azAssignPslcLs.get("azsl").asText(),//tbl_Az_Assign_Pslc_Ls.azsl)
            azAssignPslcLs.get("azwdmc").asText(),//tbl_Az_Assign_Pslc_Ls.azwdmc)
            azAssignPslcLs.get("azwdno").asText(),//tbl_Az_Assign_Pslc_Ls.azwdno)
            azAssignPslcLs.get("azwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.azwdxtbh)
            azAssignPslcLs.get("beiz").asText(),//tbl_Az_Assign_Pslc_Ls. beiz)
            azAssignPslcLs.get("bjustat").asText(),//tbl_Az_Assign_Pslc_Ls.bjustat)
            azAssignPslcLs.get("cjdt").asText(),//tbl_Az_Assign_Pslc_Ls.cjdt)
            azAssignPslcLs.get("cjren").asText(),//tbl_Az_Assign_Pslc_Ls.cjren)
            azAssignPslcLs.get("cjrmc").asText(),//tbl_Az_Assign_Pslc_Ls.cjrmc)
            azAssignPslcLs.get("cjwdno").asText(),//tbl_Az_Assign_Pslc_Ls.cjwdno)
            azAssignPslcLs.get("cjwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.cjwdxtbh)
            azAssignPslcLs.get("cshi").asText(),//tbl_Az_Assign_Pslc_Ls. cshi)
            azAssignPslcLs.get("dhhm").asText(),//tbl_Az_Assign_Pslc_Ls. dhhm)
            azAssignPslcLs.get("dizi").asText(),//tbl_Az_Assign_Pslc_Ls.dizi)
            azAssignPslcLs.get("djlxno").asText(),//tbl_Az_Assign_Pslc_Ls.djlxno)
            azAssignPslcLs.get("dqjd").asText(),//tbl_Az_Assign_Pslc_Ls.dqjd)
            azAssignPslcLs.get("dqjdsj").asText(),//tbl_Az_Assign_Pslc_Ls.dqjd)
            azAssignPslcLs.get("email").asText(),//tbl_Az_Assign_Pslc_Ls.email)
            azAssignPslcLs.get("fjhm").asText(),//tbl_Az_Assign_Pslc_Ls.fjhm)
            azAssignPslcLs.get("fphm").asText(),//tbl_Az_Assign_Pslc_Ls.fphm	)
            azAssignPslcLs.get("gcbh").asText(),//tbl_Az_Assign_Pslc_Ls.gcbh	)
            azAssignPslcLs.get("gcmc").asText(),//tbl_Az_Assign_Pslc_Ls.gcmc	)
            azAssignPslcLs.get("gdhao").asText(),//tbl_Az_Assign_Pslc_Ls.gdhao)
            azAssignPslcLs.get("gmsj").asText(),//tbl_Az_Assign_Pslc_Ls.gmsj)
            azAssignPslcLs.get("gpsdzxx").asText(),//tbl_Az_Assign_Pslc_Ls.gpsdzxx)
            azAssignPslcLs.get("jindu").asText(),//tbl_Az_Assign_Pslc_Ls.jindu)
            azAssignPslcLs.get("jspgwdmc").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdmc)
            azAssignPslcLs.get("jspgwdno").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdno)
            azAssignPslcLs.get("jspgwdsj").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdsj)
            azAssignPslcLs.get("jspgwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.jspgwdxtbh)
            azAssignPslcLs.get("kqbh").asText(),//tbl_Az_Assign_Pslc_Ls.kqbh)
            azAssignPslcLs.get("lcid").asText(),//tbl_Az_Assign_Pslc_Ls.lcid)
            azAssignPslcLs.get("lxren").asText(),//tbl_Az_Assign_Pslc_Ls.lxren)
            azAssignPslcLs.get("pgid").asText(),//tbl_Az_Assign_Pslc_Ls.pgid)
            azAssignPslcLs.get("qqlymc").asText(),//tbl_Az_Assign_Pslc_Ls.qqlymc)
            azAssignPslcLs.get("qqlyno").asText(),//tbl_Az_Assign_Pslc_Ls.qqlyno)
            azAssignPslcLs.get("qqlyxh").asText(),//tbl_Az_Assign_Pslc_Ls.qqlyxh	)
            azAssignPslcLs.get("qqlyzj").asText(),//tbl_Az_Assign_Pslc_Ls.qqlyzj)
            azAssignPslcLs.get("quhao").asText(),//tbl_Az_Assign_Pslc_Ls.quhao)
            azAssignPslcLs.get("sfen").asText(),//tbl_Az_Assign_Pslc_Ls.sfen)
            azAssignPslcLs.get("sfwcps").asText(),//tbl_Az_Assign_Pslc_Ls. sfwcps)
            azAssignPslcLs.get("sfygllc").asText(),//tbl_Az_Assign_Pslc_Ls. sfygllc)
            azAssignPslcLs.get("shsj").asText(),//tbl_Az_Assign_Pslc_Ls. shsj)
            azAssignPslcLs.get("spid").asText(),//tbl_Az_Assign_Pslc_Ls.spid)
            azAssignPslcLs.get("spmc").asText(),//tbl_Az_Assign_Pslc_Ls.spmc)
            azAssignPslcLs.get("ssqy").asText(),//tbl_Az_Assign_Pslc_Ls.ssqy)
            azAssignPslcLs.get("stat").asText(),//tbl_Az_Assign_Pslc_Ls.stat)
            azAssignPslcLs.get("syjd").asText(),//tbl_Az_Assign_Pslc_Ls.syjd	)
            azAssignPslcLs.get("weidu").asText(),//tbl_Az_Assign_Pslc_Ls.weidu	)
            azAssignPslcLs.get("wwsl").asText(),//tbl_Az_Assign_Pslc_Ls.wwsl	)
            azAssignPslcLs.get("xian").asText(),//tbl_Az_Assign_Pslc_Ls.xian)
            azAssignPslcLs.get("xsdh").asText(),//tbl_Az_Assign_Pslc_Ls.xsdh	)
            azAssignPslcLs.get("xsdwno").asText(),//tbl_Az_Assign_Pslc_Ls.xsdwno)
            azAssignPslcLs.get("xsdwxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.xsdwxtbh)
            azAssignPslcLs.get("xslx").asText(),//tbl_Az_Assign_Pslc_Ls.xslx	)
            azAssignPslcLs.get("xsorsh").asText(),//tbl_Az_Assign_Pslc_Ls.xsorsh	)
            azAssignPslcLs.get("xswdmc").asText(),//tbl_Az_Assign_Pslc_Ls.xswdmc)
            azAssignPslcLs.get("xxlb").asText(),//tbl_Az_Assign_Pslc_Ls.xxlb	)
            azAssignPslcLs.get("xxly").asText(),//tbl_Az_Assign_Pslc_Ls.xxly)
            azAssignPslcLs.get("xxqd").asText(),//tbl_Az_Assign_Pslc_Ls.xxqd	)
            azAssignPslcLs.get("xzhen").asText(),//tbl_Az_Assign_Pslc_Ls.xzhen	)
            azAssignPslcLs.get("yddh").asText(),//tbl_Az_Assign_Pslc_Ls.yddh	)
            azAssignPslcLs.get("yddh2").asText(),//tbl_Az_Assign_Pslc_Ls.yddh2		)
            azAssignPslcLs.get("yhmc").asText(),//tbl_Az_Assign_Pslc_Ls.yhmc		)
            azAssignPslcLs.get("yhqwjssj").asText(),//tbl_Az_Assign_Pslc_Ls.yhqwjssj	)
            azAssignPslcLs.get("yhqwkssj").asText(),//tbl_Az_Assign_Pslc_Ls.yhqwkssj	)
            azAssignPslcLs.get("yhsx").asText(),//tbl_Az_Assign_Pslc_Ls.yhqwkssj	)
            azAssignPslcLs.get("yxji").asText(),//tbl_Az_Assign_Pslc_Ls.yxji		)
            azAssignPslcLs.get("yyazsj").asText(),//tbl_Az_Assign_Pslc_Ls.yyazsj	)
            azAssignPslcLs.get("zjczren").asText(),//tbl_Az_Assign_Pslc_Ls.zjczren	)
            azAssignPslcLs.get("zjczsj").asText(),//tbl_Az_Assign_Pslc_Ls.zjczsj		)
            azAssignPslcLs.get("zjczwd").asText(),//tbl_Az_Assign_Pslc_Ls.zjczwd	)
            azAssignPslcLs.get("zjczwdxtbh").asText(),//tbl_Az_Assign_Pslc_Ls.zjczwdxtbh	)
            azAssignPslcLs.get("zxha").asText(),//tbl_Az_Assign_Pslc_Ls.zxha		)
            azAssignPslcLs.get("cshiid").asText(),//tbl_Az_Assign_Pslc_Ls.cshiid	)
            azAssignPslcLs.get("xzhenid").asText(),//tbl_Az_Assign_Pslc_Ls.xzhenid	)
            azAssignPslcLs.get("sfenid").asText(),//tbl_Az_Assign_Pslc_Ls.sfenid	)
            azAssignPslcLs.get("xianid").asText(),//tbl_Az_Assign_Pslc_Ls.xianid	)
            azAssignPslcLs.get("xxlyid").asText(),//tbl_Az_Assign_Pslc_Ls.xxlyid	)
            azAssignPslcLs.get("xslxid").asText(),//tbl_Az_Assign_Pslc_Ls.xslxid		)
            azAssignPslcLs.get("yhsxid").asText(),//tbl_Az_Assign_Pslc_Ls.yhsxid			)
            azAssignPslcLs.get("organizationname").asText(),//tbl_Az_Assign_Pslc_Ls.organizationname		)
            azAssignPslcLs.get("organizationaddress").asText(),//tbl_Az_Assign_Pslc_Ls.organizationaddress	)
            azAssignPslcLs.get("name").asText(),//tbl_Az_Assign_Pslc_Ls.name		)
            azAssignPslcLs.get("mobile").asText(),//tbl_Az_Assign_Pslc_Ls.mobile	)
            azAssignPslcLs.get("retailsign").asText(),//tbl_Az_Assign_Pslc_Ls.retailsign	)
            azAssignPslcLs.get("appointmentkssj").asText(),//tbl_Az_Assign_Pslc_Ls.appointmentkssj		)
            azAssignPslcLs.get("appointmentjssj").asText(),//tbl_Az_Assign_Pslc_Ls.appointmentjssj	)
            azAssignPslcLs.get("wcsj").asText(),
            azAssignPslcLs.get("requisition").asText(),
            azAssignPslcLs.get("shopno").asText(),
            azAssignPslcLs.get("shopname").asText(),
            ts,
            table
            )
          )

        } catch {
          case e: Exception => logger.error("安装看板AzAssignMxFunction抓取数据异常->" + e.getMessage + "原因" + e.getCause)
        }
      }
    }
  }
}
