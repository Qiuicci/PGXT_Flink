package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Fee
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Az_Assign_Fee_ES(tblAzAssignFee: OutputTag[Tbl_Az_Assign_Fee]) extends  ProcessFunction[ObjectNode,Tbl_Az_Assign_Fee]{
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Fee]#Context, out: Collector[Tbl_Az_Assign_Fee]): Unit = {
    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Fee_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()


    if ("DELETE".equals(caoZuoType)) {
      logger.info("AzAssignFeeFunction此数据为DELETE不做处理")
    } else {
      while (data.hasNext) {
        val feebiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Az_Assign_Fee(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("otherfee").asText(),
            feebiao.get("totalfee").asText(),
            feebiao.get("ajia").asText(),
            feebiao.get("jcguan").asText(),
            feebiao.get("kqkg").asText(),
            feebiao.get("gkzy").asText(),
            feebiao.get("yccxqk").asText(),
            feebiao.get("flbz").asText(),
            feebiao.get("pgguid").asText(),
            ts,
            table
          ))


          ctx.output[Tbl_Az_Assign_Fee](tblAzAssignFee, Tbl_Az_Assign_Fee(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("otherfee").asText(),
            feebiao.get("totalfee").asText(),
            feebiao.get("ajia").asText(),
            feebiao.get("jcguan").asText(),
            feebiao.get("kqkg").asText(),
            feebiao.get("gkzy").asText(),
            feebiao.get("yccxqk").asText(),
            feebiao.get("flbz").asText(),
            feebiao.get("pgguid").asText(),
            ts,
            table
          ))
        } catch {
          case e: Exception => logger.error("安装看板AzAssignFeeFunction抓取数据异常" + e.getMessage + "pgguid为" + feebiao.get("pgguid").asText())
        }
      }
    }
  }
}
