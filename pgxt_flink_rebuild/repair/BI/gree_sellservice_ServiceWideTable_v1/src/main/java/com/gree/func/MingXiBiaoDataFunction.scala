package com.gree.func

import java.util

import com.gree.constant.Constant
import com.gree.model.{Tbl_Assign_Model, Tbl_Assign_Mx}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class MingXiBiaoDataFunction(modeldata:OutputTag[Tbl_Assign_Model]) extends ProcessFunction[ObjectNode,Tbl_Assign_Mx] {

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Assign_Mx]#Context, out: Collector[Tbl_Assign_Mx]): Unit = {
    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(MingXiBiaoDataFunction.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("MingXiBiaoDataFunction此数据为DELETE不做处理")
    }else {
    while (data.hasNext) {
      val mingXiBiao: JsonNode = data.next()
      //需要同步的数据
      try {
        out.collect(Tbl_Assign_Mx(
          mingXiBiao.get("pgmxid").asText(),
          mingXiBiao.get("created_by").asText(),
          mingXiBiao.get("created_date").asText(),
          mingXiBiao.get("last_modified_by").asText(),
          mingXiBiao.get("last_modified_date").asText(),
          mingXiBiao.get("pgid").asText(),
          mingXiBiao.get("spid").asText(),
          mingXiBiao.get("spmc").asText(),
          mingXiBiao.get("xlid").asText(),
          mingXiBiao.get("xlmc").asText(),
          mingXiBiao.get("xiid").asText(),
          mingXiBiao.get("ximc").asText(),
          mingXiBiao.get("jxid").asText(),
          mingXiBiao.get("jxmc").asText(),
          mingXiBiao.get("jxno").asText(),
          mingXiBiao.get("gmsj").asText(),
          mingXiBiao.get("xsdw").asText(),
          mingXiBiao.get("xsdwdh").asText(),
          mingXiBiao.get("fwdw").asText(),
          mingXiBiao.get("fwdwdh").asText(),
          mingXiBiao.get("gzwz").asText(),
          mingXiBiao.get("gzxx").asText(),
          mingXiBiao.get("czren").asText(),
          mingXiBiao.get("czsj").asText(),
          mingXiBiao.get("czwd").asText(),
          mingXiBiao.get("njtm").asText(),
          mingXiBiao.get("wjtm").asText(),
          mingXiBiao.get("beiz").asText(),
          mingXiBiao.get("njtm2").asText(),
          mingXiBiao.get("qqlyxh").asText(),
          mingXiBiao.get("pinpai").asText(),
          mingXiBiao.get("fee").asText(),
          mingXiBiao.get("xxfee").asText(),
          mingXiBiao.get("hsqk").asText(),
          mingXiBiao.get("gzxxid").asText(),
          mingXiBiao.get("shul").asText(),
          mingXiBiao.get("wwsl").asText(),
          caoZuoType,ts,table,
          mingXiBiao.get("wxcount").asText(),
          mingXiBiao.get("tmjscount").asText(),
          mingXiBiao.get("yblength").asText(),
          mingXiBiao.get("bxdue").asText()
        ))

        Thread.sleep(100)

        //需要进行逻辑计算的数据
        ctx.output[Tbl_Assign_Model](modeldata, Tbl_Assign_Model(
          Constant.TBL_ASSIGN_MX_INDEX,
          Constant.TBL_ASSIGN_MX_QUERY_ID,
          mingXiBiao.get("pgmxid").asText(),
          mingXiBiao.get("pgid").asText(),
          table,ts
        ))
      } catch {
        case e: Exception => logger.error("维修看板MingXiBiaoDataFunction抓取数据出现异常->" + e.getMessage)
      }
    }
    }
  }
}