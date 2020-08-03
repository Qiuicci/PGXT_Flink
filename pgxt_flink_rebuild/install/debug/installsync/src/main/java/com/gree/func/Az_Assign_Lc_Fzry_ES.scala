package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Lc_Fzry
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Az_Assign_Lc_Fzry_ES(tblAzAssignLcFzry:OutputTag[Tbl_Az_Assign_Lc_Fzry]) extends  ProcessFunction[ObjectNode,Tbl_Az_Assign_Lc_Fzry] {
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Lc_Fzry]#Context, out: Collector[Tbl_Az_Assign_Lc_Fzry]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Lc_Fzry_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("AzAssignLcFzryFunction此数据为DELETE不做处理")
    }else{
      while(data.hasNext){
        val feebiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Az_Assign_Lc_Fzry(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("azren").asText(),
            feebiao.get("azrenid").asText(),
            feebiao.get("pgguid").asText(),
            ts,
            table
          ))


          ctx.output[Tbl_Az_Assign_Lc_Fzry](tblAzAssignLcFzry,Tbl_Az_Assign_Lc_Fzry(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("azren").asText(),
            feebiao.get("azrenid").asText(),
            feebiao.get("pgguid").asText(),
            ts,
            table
          ))
        } catch {
          case e:Exception =>logger.error("安装看板AzAssignLcFzryFunction抓取数据异常"+e.getMessage+"pgguid为"+feebiao.get("pgguid").asText())
        }
      }
    }
  }
}
