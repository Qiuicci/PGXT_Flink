package com.gree.func

import java.util

import com.gree.model.Tbl_Yjhx_Jdd
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class Yjhx_Jdd_ES (yjhxJddData:OutputTag[Tbl_Yjhx_Jdd]) extends ProcessFunction[ObjectNode,Tbl_Yjhx_Jdd]{
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Yjhx_Jdd]#Context, out: Collector[Tbl_Yjhx_Jdd]): Unit = {
    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(Yjhx_Jdd_ES.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts =  value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("AzYjhxJddFunction此数据为DELETE不做处理")
    }else{
      while(data.hasNext){
        val feebiao: JsonNode = data.next()
        //需要同步的数据
        try {
          out.collect(Tbl_Yjhx_Jdd(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("oldMachineBrand").asText(),
            feebiao.get("oldMachineType").asText(),
            feebiao.get("brandFlag").asText(),
            feebiao.get("typeFlag").asText(),
            feebiao.get("realBrand").asText(),
            feebiao.get("realType").asText(),
            feebiao.get("machineIntegrity").asText(),
            feebiao.get("tempBarcode").asText(),
            feebiao.get("tempBarcodeImg").asText(),
            feebiao.get("identifyResult").asText(),
            feebiao.get("hxjddid").asText(),
            feebiao.get("pgguid").asText(),
            feebiao.get("xsdh").asText(),
            feebiao.get("cpps").asText(),
            ts,
            table
          ))


          ctx.output[Tbl_Yjhx_Jdd](yjhxJddData,Tbl_Yjhx_Jdd(
            feebiao.get("id").asText(),
            feebiao.get("created_by").asText(),
            feebiao.get("created_date").asText(),
            feebiao.get("last_modified_by").asText(),
            feebiao.get("last_modified_date").asText(),
            feebiao.get("oldMachineBrand").asText(),
            feebiao.get("oldMachineType").asText(),
            feebiao.get("brandFlag").asText(),
            feebiao.get("typeFlag").asText(),
            feebiao.get("realBrand").asText(),
            feebiao.get("realType").asText(),
            feebiao.get("machineIntegrity").asText(),
            feebiao.get("tempBarcode").asText(),
            feebiao.get("tempBarcodeImg").asText(),
            feebiao.get("identifyResult").asText(),
            feebiao.get("hxjddid").asText(),
            feebiao.get("pgguid").asText(),
            feebiao.get("xsdh").asText(),
            feebiao.get("cpps").asText(),
            ts,
            table
          ))
        } catch {
          case e:Exception =>logger.error("安装看板AzYjhxJddFunction抓取数据异常"+e.getMessage+"pgguid为"+feebiao.get("pgguid").asText())
        }
      }
    }
  }
}
