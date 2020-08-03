package com.gree.func

import java.util

import com.gree.model.Tbl_Trade_New_For_Old
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Trade_New_For_Old_ES(tradeNewForOldData:OutputTag[Tbl_Trade_New_For_Old]) extends ProcessFunction[ObjectNode,Tbl_Trade_New_For_Old] {
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Trade_New_For_Old]#Context, out: Collector[Tbl_Trade_New_For_Old]): Unit = {
    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Trade_New_For_Old_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("AzTradeNewForOldFunction此数据为DELETE不做处理")
    }else{
      while(data.hasNext){
        val feebiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Trade_New_For_Old(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("oldMachineBrand").asText(),
            feebiao.get("oldMachineType").asText(),
            feebiao.get("oldMachineNum").asText(),
            feebiao.get("connector").asText(),
            feebiao.get("connectWay").asText(),
            feebiao.get("hxmxid").asText(),
            feebiao.get("pgguid").asText(),
            feebiao.get("xsdh").asText(),
            feebiao.get("orderitemid").asText(),
            feebiao.get("lcid").asText(),
            ts,
            table
          ))


          ctx.output[Tbl_Trade_New_For_Old](tradeNewForOldData,Tbl_Trade_New_For_Old(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("oldMachineBrand").asText(),
            feebiao.get("oldMachineType").asText(),
            feebiao.get("oldMachineNum").asText(),
            feebiao.get("connector").asText(),
            feebiao.get("connectWay").asText(),
            feebiao.get("hxmxid").asText(),
            feebiao.get("pgguid").asText(),
            feebiao.get("xsdh").asText(),
            feebiao.get("orderitemid").asText(),
            feebiao.get("lcid").asText(),
            ts,
            table
          ))
        } catch {
          case e:Exception =>logger.error("安装看板AzTradeNewForOldFunction抓取数据异常"+e.getMessage+"pgguid为"+feebiao.get("pgguid").asText())
        }
      }
    }
  }
}
