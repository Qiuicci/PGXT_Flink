package com.gree.func

import java.util

import com.gree.constant.Constant
import com.gree.model.{Tbl_Assign_FeedBack, Tbl_Assign_Model}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class FanKuiBiaoDataFunction(modeldata:OutputTag[Tbl_Assign_Model]) extends ProcessFunction[ObjectNode,Tbl_Assign_FeedBack]{

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Assign_FeedBack]#Context, out: Collector[Tbl_Assign_FeedBack]): Unit = {
    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(FanKuiBiaoDataFunction.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("FanKuiBiaoDataFunction此数据为DELETE不做处理")
    }else {
    while(data.hasNext) {
      val fanKuiData: JsonNode = data.next()
      //需要同步的数据
      try {
        out.collect(Tbl_Assign_FeedBack(
          fanKuiData.get("id").asText(),
          fanKuiData.get("created_by").asText(),
          fanKuiData.get("created_date").asText(),
          fanKuiData.get("last_modified_by").asText(),
          fanKuiData.get("last_modified_date").asText(),
          fanKuiData.get("pgid").asText(),
          fanKuiData.get("zlfksj").asText(),
          fanKuiData.get("zlfkbh").asText(),
          fanKuiData.get("czren").asText(),
          fanKuiData.get("czsj").asText(),
          caoZuoType,ts,table
        ))

        Thread.sleep(100)

        //需要进行逻辑计算的数据
        ctx.output[Tbl_Assign_Model](modeldata,
          Tbl_Assign_Model(
            Constant.TBL_ASSIGN_FEEDBACK_INDEX,
            Constant.TBL_ASSIGN_FEEDBACK_QUERY_ID,
            fanKuiData.get("id").asText(),
            fanKuiData.get("pgid").asText(),
            table,ts

          )
        )
      } catch {
        case e: Exception => logger.error("维修看板FanKuiBiaoDataFunction抓取数据出现异常->" + e.getMessage)
      }
    }
    }
  }

}
