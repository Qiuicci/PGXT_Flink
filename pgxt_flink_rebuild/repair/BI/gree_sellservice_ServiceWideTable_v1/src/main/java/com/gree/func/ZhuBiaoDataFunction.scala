package com.gree.func

import java.util

import com.gree.constant.Constant
import com.gree.model.{Tbl_Assign, Tbl_Assign_Model}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class ZhuBiaoDataFunction(modeldata:OutputTag[Tbl_Assign_Model]) extends ProcessFunction[ObjectNode,Tbl_Assign]{
  val logger: Logger = LoggerFactory.getLogger(ZhuBiaoDataFunction.super.getClass)
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Assign]#Context, out: Collector[Tbl_Assign]): Unit = {

    //data中可能有多个对象，遍历取出
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("ZhuBiaoDataFunction此数据为DELETE不做处理")
    }else {
      while (data.hasNext) {
        val zhubiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Assign(
            zhubiao.get("pgid").asText(),
            zhubiao.get("created_by").asText(),
            zhubiao.get("created_date").asText(),
            zhubiao.get("last_modified_by").asText(),
            zhubiao.get("last_modified_date").asText(),
            zhubiao.get("auth_state").asText(),
            zhubiao.get("azsl").asText(),
            zhubiao.get("beiz").asText(),
            zhubiao.get("bjustat").asText(),
            zhubiao.get("chaoshiqe").asText(),
            zhubiao.get("cjdt").asText(),
            zhubiao.get("cjren").asText(),
            zhubiao.get("cjrmc").asText(),
            zhubiao.get("cjwdno").asText(),
            zhubiao.get("cshi").asText(),
            zhubiao.get("cshiid").asText(),
            zhubiao.get("cxyzm").asText(),
            zhubiao.get("dhhm").asText(),
            zhubiao.get("dizi").asText(),
            zhubiao.get("dqjdsj").asText(),
            zhubiao.get("email").asText(),
            zhubiao.get("fjhm").asText(),
            zhubiao.get("fwrybwgsj").asText(),
            zhubiao.get("gdhao").asText(),
            zhubiao.get("gpsdzxx").asText(),
            zhubiao.get("jindu").asText(),
            zhubiao.get("ldcs").asText(),
            zhubiao.get("pgguid").asText(),
            zhubiao.get("qqlymc").asText(),
            zhubiao.get("qqlyxh").asText(),
            zhubiao.get("qqlyzj").asText(),
            zhubiao.get("quhao").asText(),
            zhubiao.get("qwsmjssj").asText(),
            zhubiao.get("qystat").asText(),
            zhubiao.get("sfen").asText(),
            zhubiao.get("sfenid").asText(),
            zhubiao.get("sffswx").asText(),
            zhubiao.get("spid").asText(),
            zhubiao.get("spmc").asText(),
            zhubiao.get("ssqy").asText(),
            zhubiao.get("stat").asText(),
            zhubiao.get("tsdengji").asText(),
            zhubiao.get("wcsj").asText(),
            zhubiao.get("weidu").asText(),
            zhubiao.get("wwsl").asText(),
            zhubiao.get("wxren").asText(),
            zhubiao.get("wxrenid").asText(),
            zhubiao.get("wxshul").asText(),
            zhubiao.get("wxwdmc").asText(),
            zhubiao.get("wxwdno").asText(),
            zhubiao.get("xian").asText(),
            zhubiao.get("xianid").asText(),
            zhubiao.get("xjwdmc").asText(),
            zhubiao.get("xjwdno").asText(),
            zhubiao.get("xjwdsj").asText(),
            zhubiao.get("xqxiaolei").asText(),
            zhubiao.get("xsdh").asText(),
            zhubiao.get("xsorsh").asText(),
            zhubiao.get("xswdmc").asText(),
            zhubiao.get("xswdno").asText(),
            zhubiao.get("xxlb").asText(),
            zhubiao.get("xxlbid").asText(),
            zhubiao.get("xxly").asText(),
            zhubiao.get("xxlyid").asText(),
            zhubiao.get("xxqd").asText(),
            zhubiao.get("xxqdid").asText(),
            zhubiao.get("xzhen").asText(),
            zhubiao.get("yddh").asText(),
            zhubiao.get("yddh2").asText(),
            zhubiao.get("yhgyhf").asText(),
            zhubiao.get("yhif").asText(),
            zhubiao.get("yhmc").asText(),
            zhubiao.get("yhqwsmsj").asText(),
            zhubiao.get("yhsx").asText(),
            zhubiao.get("yhyyczsj").asText(),
            zhubiao.get("yxji").asText(),
            zhubiao.get("zbby").asText(),
            zhubiao.get("zjczsj").asText(),
            zhubiao.get("zjczwd").asText(),
            zhubiao.get("zjczwdxtbh").asText(),
            zhubiao.get("zptype").asText(),
            zhubiao.get("zxhao").asText(),
            zhubiao.get("yhsxid").asText(),
            zhubiao.get("xzhenid").asText(),
            zhubiao.get("wxcount").asText(),
            zhubiao.get("extjson1").asText(),
            zhubiao.get("extjson2").asText(),
            zhubiao.get("extjson3").asText(),
            zhubiao.get("extjson4").asText(),
            zhubiao.get("extjson5").asText(),
            zhubiao.get("vip").asText(),
            caoZuoType,ts,table))

          Thread.sleep(200)

          //需要进行逻辑计算的数据
          ctx.output[Tbl_Assign_Model](modeldata, Tbl_Assign_Model(Constant.TBL_ASSIGN_INDEX,Constant.TBL_ASSIGN_QUERY_ID,zhubiao.get("pgid").asText(),zhubiao.get("pgid").asText(),table,ts))
        } catch {
          case e: Exception => logger.error("维修看板ZhuBiaoDataFunction抓取数据异常->" + e.getMessage)
        }
      }
    }
  }
}
