package com.gree.func

import java.util

import com.gree.model.Tbl_Az_Assign_Appointment
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Az_Assign_Appointment_ES(tblAzAssignAppointment: OutputTag[Tbl_Az_Assign_Appointment]) extends ProcessFunction[ObjectNode,Tbl_Az_Assign_Appointment]{
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Az_Assign_Appointment]#Context, out: Collector[Tbl_Az_Assign_Appointment]): Unit = {

    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Az_Assign_Appointment_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("YuYueBiaoDataFunction此数据为DELETE不做处理")
    }else {
      while (data.hasNext) {
        val yuyuebiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Az_Assign_Appointment(
            yuyuebiao.get("id").asText(),
            yuyuebiao.get("created_by").asText(),
            yuyuebiao.get("created_date").asText(),
            yuyuebiao.get("last_modified_by").asText(),
            yuyuebiao.get("last_modified_date").asText(),
            yuyuebiao.get("kssj").asText(),
            yuyuebiao.get("jssj").asText(),
            yuyuebiao.get("czren").asText(),
            yuyuebiao.get("pgguid").asText(),
            yuyuebiao.get("czsj").asText(),
            yuyuebiao.get("leix").asText(),
            yuyuebiao.get("reason").asText(),
            yuyuebiao.get("beiz").asText(),
            caoZuoType,
            ts,
            table))


          //需要进行逻辑计算的数据
          ctx.output[Tbl_Az_Assign_Appointment](tblAzAssignAppointment, Tbl_Az_Assign_Appointment(
            yuyuebiao.get("id").asText(),
            yuyuebiao.get("created_by").asText(),
            yuyuebiao.get("created_date").asText(),
            yuyuebiao.get("last_modified_by").asText(),
            yuyuebiao.get("last_modified_date").asText(),
            yuyuebiao.get("kssj").asText(),
            yuyuebiao.get("jssj").asText(),
            yuyuebiao.get("czren").asText(),
            yuyuebiao.get("pgguid").asText(),
            yuyuebiao.get("czsj").asText(),
            yuyuebiao.get("leix").asText(),
            yuyuebiao.get("reason").asText(),
            yuyuebiao.get("beiz").asText(),
            caoZuoType,
            ts,
            table))
        } catch {
          case e: Exception => logger.error("安装看板YuYueBiaoDataFunction抓取数据异常" + e.getMessage)
        }

      }
    }
  }
}
