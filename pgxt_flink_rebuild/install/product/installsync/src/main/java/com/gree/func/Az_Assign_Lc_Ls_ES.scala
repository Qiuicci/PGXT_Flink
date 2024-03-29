package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Lc_Ls
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Az_Assign_Lc_Ls_ES(tblAzAssignLcLs: OutputTag[Tbl_Az_Assign_Lc_Ls])  extends ProcessFunction [ObjectNode,Tbl_Az_Assign_Lc_Ls]{
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Lc_Ls]#Context, out: Collector[Tbl_Az_Assign_Lc_Ls]): Unit = {

    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Lc_Ls_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("ZhuBiaoDataFunction此数据为DELETE不做处理")
    }else{
      while(data.hasNext){
        val zhubiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Az_Assign_Lc_Ls(
            zhubiao.get("pgguid").asText(),
            zhubiao.get("created_by").asText(),
            zhubiao.get("created_date").asText(),
            zhubiao.get("last_modified_by").asText(),
            zhubiao.get("last_modified_date").asText(),
            zhubiao.get("pgid").asText(),
            zhubiao.get("yhmc").asText(),
            zhubiao.get("yddh").asText(),
            zhubiao.get("yddh2").asText(),
            zhubiao.get("quhao").asText(),
            zhubiao.get("dhhm").asText(),
            zhubiao.get("fjhm").asText(),
            zhubiao.get("email").asText(),
            zhubiao.get("sfen").asText(),
            zhubiao.get("cshi").asText(),
            zhubiao.get("xian").asText(),
            zhubiao.get("xzhen").asText(),
            zhubiao.get("dizi").asText(),
            zhubiao.get("xxqd").asText(),
            zhubiao.get("xxly").asText(),
            zhubiao.get("xxlb").asText(),
            zhubiao.get("beiz").asText(),
            zhubiao.get("yhsx").asText(),
            zhubiao.get("yxji").asText(),
            zhubiao.get("gdhao").asText(),
            zhubiao.get("spid").asText(),
            zhubiao.get("spmc").asText(),
            zhubiao.get("azren").asText(),
            zhubiao.get("azrenid").asText(),
            zhubiao.get("azwdxtbh").asText(),
            zhubiao.get("azwdno").asText(),
            zhubiao.get("azwdmc").asText(),
            zhubiao.get("jspgwdno").asText(),
            zhubiao.get("jspgwdmc").asText(),
            zhubiao.get("jspgwdxtbh").asText(),
            zhubiao.get("jspgwdsj").asText(),
            zhubiao.get("zxha").asText(),
            zhubiao.get("ssqy").asText(),
            zhubiao.get("qqlyno").asText(),
            zhubiao.get("qqlymc").asText(),
            zhubiao.get("qqlyxh").asText(),
            zhubiao.get("qqlyzj").asText(),
            zhubiao.get("bjustat").asText(),
            zhubiao.get("yhqwkssj").asText(),
            zhubiao.get("yhqwjssj").asText(),
            zhubiao.get("stat").asText(),
            zhubiao.get("gpsdzxx").asText(),
            zhubiao.get("cjren").asText(),
            zhubiao.get("cjrmc").asText(),
            zhubiao.get("cjdt").asText(),
            zhubiao.get("cjwdno").asText(),
            zhubiao.get("cjwdxtbh").asText(),
            zhubiao.get("zjczren").asText(),
            zhubiao.get("zjczwd").asText(),
            zhubiao.get("zjczwdxtbh").asText(),
            zhubiao.get("zjczsj").asText(),
            zhubiao.get("xslx").asText(),
            zhubiao.get("lcid").asText(),
            zhubiao.get("djlxno").asText(),
            zhubiao.get("yyazsj").asText(),
            zhubiao.get("sfwcps").asText(),
            zhubiao.get("xsdh").asText(),
            zhubiao.get("gcbh").asText(),
            zhubiao.get("gcmc").asText(),
            zhubiao.get("azsl").asText(),
            zhubiao.get("wwsl").asText(),
            zhubiao.get("lxren").asText(),
            zhubiao.get("xsdwno").asText(),
            zhubiao.get("xswdmc").asText(),
            zhubiao.get("xsdwxtbh").asText(),
            zhubiao.get("fphm").asText(),
            zhubiao.get("gmsj").asText(),
            zhubiao.get("kqbh").asText(),
            zhubiao.get("xsorsh").asText(),
            zhubiao.get("dqjdsj").asText(),
            zhubiao.get("syjd").asText(),
            zhubiao.get("dqjd").asText(),
            zhubiao.get("jindu").asText(),
            zhubiao.get("weidu").asText(),
            zhubiao.get("shsj").asText(),
            zhubiao.get("sfygllc").asText(),
            zhubiao.get("xslxid").asText(),
            zhubiao.get("yhsxid").asText(),
            zhubiao.get("xxlbid").asText(),
            zhubiao.get("xxlyid").asText(),
            zhubiao.get("sfenid").asText(),
            zhubiao.get("cshiid").asText(),
            zhubiao.get("xianid").asText(),
            zhubiao.get("xzhenid").asText(),
            zhubiao.get("wcsj").asText(),
            zhubiao.get("retailsign").asText(),
            zhubiao.get("servicewdmc").asText(),
            zhubiao.get("shopname").asText(),
            zhubiao.get("orderphone").asText(),
            zhubiao.get("extendfiled1").asText(),
            zhubiao.get("extendfiled2").asText(),
            zhubiao.get("extendfiled3").asText(),
            zhubiao.get("extendfiled4").asText(),
            zhubiao.get("servicewdno").asText(),
            zhubiao.get("shopno").asText(),
            zhubiao.get("extendfiled5").asText(),
            zhubiao.get("organizationaddress").asText(),
            zhubiao.get("organizationname").asText(),
            caoZuoType,
            ts,
            table))


          //需要进行逻辑计算的数据
          ctx.output[Tbl_Az_Assign_Lc_Ls](tblAzAssignLcLs, Tbl_Az_Assign_Lc_Ls(
            zhubiao.get("pgguid").asText(),
            zhubiao.get("created_by").asText(),
            zhubiao.get("created_date").asText(),
            zhubiao.get("last_modified_by").asText(),
            zhubiao.get("last_modified_date").asText(),
            zhubiao.get("pgid").asText(),
            zhubiao.get("yhmc").asText(),
            zhubiao.get("yddh").asText(),
            zhubiao.get("yddh2").asText(),
            zhubiao.get("quhao").asText(),
            zhubiao.get("dhhm").asText(),
            zhubiao.get("fjhm").asText(),
            zhubiao.get("email").asText(),
            zhubiao.get("sfen").asText(),
            zhubiao.get("cshi").asText(),
            zhubiao.get("xian").asText(),
            zhubiao.get("xzhen").asText(),
            zhubiao.get("dizi").asText(),
            zhubiao.get("xxqd").asText(),
            zhubiao.get("xxly").asText(),
            zhubiao.get("xxlb").asText(),
            zhubiao.get("beiz").asText(),
            zhubiao.get("yhsx").asText(),
            zhubiao.get("yxji").asText(),
            zhubiao.get("gdhao").asText(),
            zhubiao.get("spid").asText(),
            zhubiao.get("spmc").asText(),
            zhubiao.get("azren").asText(),
            zhubiao.get("azrenid").asText(),
            zhubiao.get("azwdxtbh").asText(),
            zhubiao.get("azwdno").asText(),
            zhubiao.get("azwdmc").asText(),
            zhubiao.get("jspgwdno").asText(),
            zhubiao.get("jspgwdmc").asText(),
            zhubiao.get("jspgwdxtbh").asText(),
            zhubiao.get("jspgwdsj").asText(),
            zhubiao.get("zxha").asText(),
            zhubiao.get("ssqy").asText(),
            zhubiao.get("qqlyno").asText(),
            zhubiao.get("qqlymc").asText(),
            zhubiao.get("qqlyxh").asText(),
            zhubiao.get("qqlyzj").asText(),
            zhubiao.get("bjustat").asText(),
            zhubiao.get("yhqwkssj").asText(),
            zhubiao.get("yhqwjssj").asText(),
            zhubiao.get("stat").asText(),
            zhubiao.get("gpsdzxx").asText(),
            zhubiao.get("cjren").asText(),
            zhubiao.get("cjrmc").asText(),
            zhubiao.get("cjdt").asText(),
            zhubiao.get("cjwdno").asText(),
            zhubiao.get("cjwdxtbh").asText(),
            zhubiao.get("zjczren").asText(),
            zhubiao.get("zjczwd").asText(),
            zhubiao.get("zjczwdxtbh").asText(),
            zhubiao.get("zjczsj").asText(),
            zhubiao.get("xslx").asText(),
            zhubiao.get("lcid").asText(),
            zhubiao.get("djlxno").asText(),
            zhubiao.get("yyazsj").asText(),
            zhubiao.get("sfwcps").asText(),
            zhubiao.get("xsdh").asText(),
            zhubiao.get("gcbh").asText(),
            zhubiao.get("gcmc").asText(),
            zhubiao.get("azsl").asText(),
            zhubiao.get("wwsl").asText(),
            zhubiao.get("lxren").asText(),
            zhubiao.get("xsdwno").asText(),
            zhubiao.get("xswdmc").asText(),
            zhubiao.get("xsdwxtbh").asText(),
            zhubiao.get("fphm").asText(),
            zhubiao.get("gmsj").asText(),
            zhubiao.get("kqbh").asText(),
            zhubiao.get("xsorsh").asText(),
            zhubiao.get("dqjdsj").asText(),
            zhubiao.get("syjd").asText(),
            zhubiao.get("dqjd").asText(),
            zhubiao.get("jindu").asText(),
            zhubiao.get("weidu").asText(),
            zhubiao.get("shsj").asText(),
            zhubiao.get("sfygllc").asText(),
            zhubiao.get("xslxid").asText(),
            zhubiao.get("yhsxid").asText(),
            zhubiao.get("xxlbid").asText(),
            zhubiao.get("xxlyid").asText(),
            zhubiao.get("sfenid").asText(),
            zhubiao.get("cshiid").asText(),
            zhubiao.get("xianid").asText(),
            zhubiao.get("xzhenid").asText(),
            zhubiao.get("wcsj").asText(),
            zhubiao.get("retailsign").asText(),
            zhubiao.get("servicewdmc").asText(),
            zhubiao.get("shopname").asText(),
            zhubiao.get("orderphone").asText(),
            zhubiao.get("extendfiled1").asText(),
            zhubiao.get("extendfiled2").asText(),
            zhubiao.get("extendfiled3").asText(),
            zhubiao.get("extendfiled4").asText(),
            zhubiao.get("servicewdno").asText(),
            zhubiao.get("shopno").asText(),
            zhubiao.get("extendfiled5").asText(),
            zhubiao.get("organizationaddress").asText(),
            zhubiao.get("organizationname").asText(),
            caoZuoType,
            ts,
            table))
        } catch {
          case e:Exception =>logger.error("安装看板ZhuBiaoDataFunction抓取数据异常"+e.getMessage)
        }
      }
    }
  }

}
