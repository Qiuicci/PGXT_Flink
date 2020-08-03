package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Satisfaction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Az_Assign_Satisfaction_ES(tblAzAssignSatisfaction: OutputTag[Tbl_Az_Assign_Satisfaction]) extends ProcessFunction[ObjectNode, Tbl_Az_Assign_Satisfaction] {
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Satisfaction]#Context, out: Collector[Tbl_Az_Assign_Satisfaction]): Unit = {

    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Satisfaction_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("AzAssignSatisfactionFunction此数据为DELETE不做处理")
    }else {
      while (data.hasNext) {
        val zhubiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Az_Assign_Satisfaction(
            zhubiao.get("id").asText(),
            zhubiao.get("created_by").asText(),
            zhubiao.get("created_date").asText(),
            zhubiao.get("last_modified_by").asText(),
            zhubiao.get("last_modified_date").asText(),
            zhubiao.get("pgguid").asText(),
            zhubiao.get("pjly").asText(),
            zhubiao.get("pjnr").asText(),
            zhubiao.get("hfren").asText(),
            zhubiao.get("hfwdmc").asText(),
            zhubiao.get("hfwdno").asText(),
            zhubiao.get("hfsj").asText(),
            zhubiao.get("bmylx").asText(),
            zhubiao.get("bmybeiz").asText(),
            zhubiao.get("bmysj").asText(),
            zhubiao.get("splb").asText(),
            zhubiao.get("mydlx").asText(),
            zhubiao.get("sxlx").asText(),
            ts,
            table))


          //需要进行逻辑计算的数据
          ctx.output[Tbl_Az_Assign_Satisfaction](tblAzAssignSatisfaction, Tbl_Az_Assign_Satisfaction(
            zhubiao.get("id").asText(),
            zhubiao.get("created_by").asText(),
            zhubiao.get("created_date").asText(),
            zhubiao.get("last_modified_by").asText(),
            zhubiao.get("last_modified_date").asText(),
            zhubiao.get("pgguid").asText(),
            zhubiao.get("pjly").asText(),
            zhubiao.get("pjnr").asText(),
            zhubiao.get("hfren").asText(),
            zhubiao.get("hfwdmc").asText(),
            zhubiao.get("hfwdno").asText(),
            zhubiao.get("hfsj").asText(),
            zhubiao.get("bmylx").asText(),
            zhubiao.get("bmybeiz").asText(),
            zhubiao.get("bmysj").asText(),
            zhubiao.get("splb").asText(),
            zhubiao.get("mydlx").asText(),
            zhubiao.get("sxlx").asText(),
            ts,
            table))
        } catch {
          case e: Exception => logger.error("安装看板AzAssignSatisfactionFunction抓取数据异常->" + e.getMessage)
        }
      }
    }
  }

}
