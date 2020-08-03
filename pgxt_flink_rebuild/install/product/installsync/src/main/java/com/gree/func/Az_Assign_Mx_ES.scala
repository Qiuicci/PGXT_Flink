package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Mx
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Az_Assign_Mx_ES(tblAzAssignMx: OutputTag[Tbl_Az_Assign_Mx]) extends ProcessFunction[ObjectNode, Tbl_Az_Assign_Mx] {
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Mx]#Context, out: Collector[Tbl_Az_Assign_Mx]): Unit = {

    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Mx_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()


    if ("DELETE".equals(caoZuoType)) {
      logger.info("AzAssignMxFunction此数据为DELETE不做处理")
    } else {
      while (data.hasNext) {
        val zhubiao: JsonNode = data.next()
        //需要同步的数据
        try {

          out.collect(Tbl_Az_Assign_Mx(
            zhubiao.get("pgmxid").asText(),
            zhubiao.get("created_by").asText(),
            zhubiao.get("created_date").asText(),
            zhubiao.get("last_modified_by").asText(),
            zhubiao.get("last_modified_date").asText(),
            zhubiao.get("pgguid").asText(),
            zhubiao.get("spid").asText(),
            zhubiao.get("spmc").asText(),
            zhubiao.get("xlid").asText(),
            zhubiao.get("xlmc").asText(),
            zhubiao.get("xiid").asText(),
            zhubiao.get("ximc").asText(),
            zhubiao.get("jxmc").asText(),
            zhubiao.get("jxno").asText(),
            zhubiao.get("czren").asText(),
            zhubiao.get("czsj").asText(),
            zhubiao.get("czwd").asText(),
            zhubiao.get("njtm").asText(),
            zhubiao.get("wjtm").asText(),
            zhubiao.get("beiz").asText(),
            zhubiao.get("shul").asText(),
            zhubiao.get("cjdt").asText(),
            zhubiao.get("jiage").asText(),
            zhubiao.get("danw").asText(),
            zhubiao.get("wldm").asText(),
            zhubiao.get("njtm2").asText(),
            zhubiao.get("wjsl").asText(),
            zhubiao.get("njsl").asText(),
            zhubiao.get("wwsl").asText(),
            ts,
            table))

          //需要进行逻辑计算的数据

          ctx.output[Tbl_Az_Assign_Mx](tblAzAssignMx, Tbl_Az_Assign_Mx(
            zhubiao.get("pgmxid").asText(),
            zhubiao.get("created_by").asText(),
            zhubiao.get("created_date").asText(),
            zhubiao.get("last_modified_by").asText(),
            zhubiao.get("last_modified_date").asText(),
            zhubiao.get("pgguid").asText(),
            zhubiao.get("spid").asText(),
            zhubiao.get("spmc").asText(),
            zhubiao.get("xlid").asText(),
            zhubiao.get("xlmc").asText(),
            zhubiao.get("xiid").asText(),
            zhubiao.get("ximc").asText(),
            zhubiao.get("jxmc").asText(),
            zhubiao.get("jxno").asText(),
            zhubiao.get("czren").asText(),
            zhubiao.get("czsj").asText(),
            zhubiao.get("czwd").asText(),
            zhubiao.get("njtm").asText(),
            zhubiao.get("wjtm").asText(),
            zhubiao.get("beiz").asText(),
            zhubiao.get("shul").asText(),
            zhubiao.get("cjdt").asText(),
            zhubiao.get("jiage").asText(),
            zhubiao.get("danw").asText(),
            zhubiao.get("wldm").asText(),
            zhubiao.get("njtm2").asText(),
            zhubiao.get("wjsl").asText(),
            zhubiao.get("njsl").asText(),
            zhubiao.get("wwsl").asText(),
            ts,
            table))

        } catch {
          case e: Exception => logger.error("安装看板AzAssignMxFunction抓取数据异常->" + e.getMessage + "原因" + e.getCause)
        }
      }
    }
  }
}
