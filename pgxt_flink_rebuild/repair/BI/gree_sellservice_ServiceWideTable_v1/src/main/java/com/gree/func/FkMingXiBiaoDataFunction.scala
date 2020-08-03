package com.gree.func

import java.util

import com.gree.constant.Constant
import com.gree.model.{Tbl_Assign_Fkmx, Tbl_Assign_Model}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class FkMingXiBiaoDataFunction(modeldata:OutputTag[Tbl_Assign_Model]) extends ProcessFunction[ObjectNode,Tbl_Assign_Fkmx]{

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Assign_Fkmx]#Context, out: Collector[Tbl_Assign_Fkmx]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(FkMingXiBiaoDataFunction.super.getClass)

    //data中可能有多个对象，遍历取出
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("FkMingXiBiaoDataFunction此数据为DELETE不做处理")
    }else {
      while (data.hasNext) {
        val fkMingXiData: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Assign_Fkmx(
            fkMingXiData.get("fkid").asText(), fkMingXiData.get("created_by").asText(), fkMingXiData.get("created_date").asText(), fkMingXiData.get("last_modified_by").asText(),
            fkMingXiData.get("last_modified_date").asText(), fkMingXiData.get("pgid").asText(), fkMingXiData.get("fklb").asText(), fkMingXiData.get("fkjg").asText(),
            fkMingXiData.get("fknr").asText(), fkMingXiData.get("fkren").asText(), fkMingXiData.get("fkrenmc").asText(), fkMingXiData.get("fksj").asText(),
            fkMingXiData.get("fkwdno").asText(), fkMingXiData.get("fkwdmc").asText(), fkMingXiData.get("scid").asText(), fkMingXiData.get("scwj").asText(),
            fkMingXiData.get("qqlyxh").asText(), fkMingXiData.get("fkmxguid").asText(), fkMingXiData.get("wjid").asText(), caoZuoType,ts,table))

          Thread.sleep(100)

          //需要进行逻辑计算的数据
          ctx.output[Tbl_Assign_Model](modeldata, Tbl_Assign_Model(
            Constant.TBL_ASSIGN_FKMX_INDEX,
            Constant.TBL_ASSIGN_FKMX_QUERY_ID,
            fkMingXiData.get("fkid").asText(),
            fkMingXiData.get("pgid").asText(),
            table,ts
          ))
        } catch {
          case e: Exception => logger.error("维修看板FkMingXiBiaoDataFunction抓取数据异常->" + e.getMessage)
        }
      }
    }
  }

}
