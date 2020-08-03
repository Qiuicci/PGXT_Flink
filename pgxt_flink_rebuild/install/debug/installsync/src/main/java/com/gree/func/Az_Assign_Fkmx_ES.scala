package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Fkmx
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

//处理反馈明细表的数据
class Az_Assign_Fkmx_ES(tblAzAssignFkmx: OutputTag[Tbl_Az_Assign_Fkmx]) extends ProcessFunction[ObjectNode,Tbl_Az_Assign_Fkmx]{

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Fkmx]#Context, out: Collector[Tbl_Az_Assign_Fkmx]): Unit = {

    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Fkmx_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("FkMingXiBiaoDataFunction此数据为DELETE不做处理")
    }else {
      while (data.hasNext) {
        val fkMingXiData: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Az_Assign_Fkmx(
            fkMingXiData.get("fkid").asText(),
            fkMingXiData.get("created_by").asText(),
            fkMingXiData.get("created_date").asText(),
            fkMingXiData.get("last_modified_by").asText(),
            fkMingXiData.get("last_modified_date").asText(),
            fkMingXiData.get("fklb").asText(),
            fkMingXiData.get("fkjg").asText(),
            fkMingXiData.get("fknr").asText(),
            fkMingXiData.get("fkren").asText(),
            fkMingXiData.get("fkrenmc").asText(),
            fkMingXiData.get("fksj").asText(),
            fkMingXiData.get("xtwdbh").asText(),
            fkMingXiData.get("wdno").asText(),
            fkMingXiData.get("wdmc").asText(),
            fkMingXiData.get("pgguid").asText(),
            fkMingXiData.get("cjdt").asText(),
            caoZuoType,
            ts,
            table))


          //需要进行逻辑计算的数据
          ctx.output[Tbl_Az_Assign_Fkmx](tblAzAssignFkmx, Tbl_Az_Assign_Fkmx(
            fkMingXiData.get("fkid").asText(),
            fkMingXiData.get("created_by").asText(),
            fkMingXiData.get("created_date").asText(),
            fkMingXiData.get("last_modified_by").asText(),
            fkMingXiData.get("last_modified_date").asText(),
            fkMingXiData.get("fklb").asText(),
            fkMingXiData.get("fkjg").asText(),
            fkMingXiData.get("fknr").asText(),
            fkMingXiData.get("fkren").asText(),
            fkMingXiData.get("fkrenmc").asText(),
            fkMingXiData.get("fksj").asText(),
            fkMingXiData.get("xtwdbh").asText(),
            fkMingXiData.get("wdno").asText(),
            fkMingXiData.get("wdmc").asText(),
            fkMingXiData.get("pgguid").asText(),
            fkMingXiData.get("cjdt").asText(),
            caoZuoType,
            ts,
            table))
        } catch {
          case e: Exception => logger.error("安装看板FkMingXiBiaoDataFunction抓取数据出现异常->" + e.getMessage)
        }
      }
    }
  }
}
