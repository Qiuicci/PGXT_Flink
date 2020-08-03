package com.gree.func

import java.util

import com.gree.constant.Constant
import com.gree.model.{Tbl_Assign_Model, Tbl_Assign_Satisfaction}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class ManYiBiaoDataFunction(modeldata:OutputTag[Tbl_Assign_Model]) extends ProcessFunction[ObjectNode,Tbl_Assign_Satisfaction]{

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Assign_Satisfaction]#Context, out: Collector[Tbl_Assign_Satisfaction]): Unit = {

    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(ManYiBiaoDataFunction.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("ManYiBiaoDataFunction此数据为DELETE不做处理")
    }else {
    while (data.hasNext) {
      val manYiBiao: JsonNode = data.next()
      //需要同步的数据
      try {
        out.collect(Tbl_Assign_Satisfaction(
          manYiBiao.get("id").asText(),
          manYiBiao.get("created_by").asText(),
          manYiBiao.get("created_date").asText(),
          manYiBiao.get("last_modified_by").asText(),
          manYiBiao.get("last_modified_date").asText(),
          manYiBiao.get("pgid").asText(),
          manYiBiao.get("pjly").asText(),
          manYiBiao.get("pjnr").asText(),
          manYiBiao.get("hfren").asText(),
          manYiBiao.get("hfwdmc").asText(),
          manYiBiao.get("hfwdno").asText(),
          manYiBiao.get("hfsj").asText(),
          manYiBiao.get("bmylx").asText(),
          manYiBiao.get("bmybeiz").asText(),
          manYiBiao.get("bmysj").asText(),
          manYiBiao.get("splb").asText(),
          manYiBiao.get("mydlx").asText(),
          manYiBiao.get("sxlx").asText(),
          caoZuoType,ts,table
        ))

        Thread.sleep(100)

        //需要进行逻辑计算的数据
        ctx.output[Tbl_Assign_Model](modeldata, Tbl_Assign_Model(
          Constant.TBL_ASSIGN_SATISFACTION_INDEX,
          Constant.TBL_ASSIGN_SATISFACTION_QUERY_ID,
          manYiBiao.get("id").asText(),
          manYiBiao.get("pgid").asText(),
          table,ts
        ))
      } catch {
        case e: Exception => logger.error("维修看板ManYiBiaoDataFunction抓取数据出现异常->" + e.getMessage)
      }
    }
    }
  }

}
