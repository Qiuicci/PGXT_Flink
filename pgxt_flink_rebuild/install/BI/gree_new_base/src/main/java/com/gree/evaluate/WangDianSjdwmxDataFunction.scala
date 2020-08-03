package com.gree.evaluate

import java.util
import com.gree.model.TblWangdianSjdwmx

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class WangDianSjdwmxDataFunction(tblWangdianSjdwmxData : OutputTag[TblWangdianSjdwmx]) extends ProcessFunction[ObjectNode,TblWangdianSjdwmx]{
  val logger: Logger = LoggerFactory.getLogger(WangDianSjdwmxDataFunction.super.getClass)

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, TblWangdianSjdwmx]#Context, out: Collector[TblWangdianSjdwmx]): Unit = {

    //data中可能有多个对象，遍历取出
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("WangDianSjdwmxDataFunction此数据为DELETE不做处理")
    }else {
      while(data.hasNext) {
        val wangdianbiao : JsonNode = data.next()
       // if("售后".equals(wangdianbiao.get("fwlb").asText()) && "2".equals(wangdianbiao.get("stat").asText())) {
          //需要同步的数据
          try {
            out.collect(TblWangdianSjdwmx(
              wangdianbiao.get("id").asText(),
              wangdianbiao.get("created_by").asText(),
              wangdianbiao.get("created_date").asText(),
              wangdianbiao.get("last_modified_by").asText(),
              wangdianbiao.get("last_modified_date").asText(),
              wangdianbiao.get("xhid").asText(),
              wangdianbiao.get("xtwdbh").asText(),
              wangdianbiao.get("wdno").asText(),
              wangdianbiao.get("splb").asText(),
              wangdianbiao.get("scqy").asText(),
              wangdianbiao.get("fwlb").asText(),
              wangdianbiao.get("sjwdno").asText(),
              wangdianbiao.get("sjwdmc").asText(),
              wangdianbiao.get("sjwdxtbh").asText(),
              wangdianbiao.get("cxqyfw").asText(),
              wangdianbiao.get("stat").asText(),
              wangdianbiao.get("czren").asText(),
              wangdianbiao.get("czrmc").asText(),
              wangdianbiao.get("czsj").asText(),
              wangdianbiao.get("zhczsj").asText(),
              wangdianbiao.get("cxqyfwbh").asText(),
              wangdianbiao.get("sfxyzq").asText(),
              wangdianbiao.get("sfxyzqbak").asText(),
              wangdianbiao.get("seno").asText(),
              wangdianbiao.get("leix").asText(),
              wangdianbiao.get("wdmc").asText(),
              wangdianbiao.get("spmc").asText(),
              wangdianbiao.get("scqymc").asText(),
              caoZuoType))


            //需要进行逻辑计算的数据
            ctx.output[TblWangdianSjdwmx](tblWangdianSjdwmxData, TblWangdianSjdwmx(
              wangdianbiao.get("id").asText(),
              wangdianbiao.get("created_by").asText(),
              wangdianbiao.get("created_date").asText(),
              wangdianbiao.get("last_modified_by").asText(),
              wangdianbiao.get("last_modified_date").asText(),
              wangdianbiao.get("xhid").asText(),
              wangdianbiao.get("xtwdbh").asText(),
              wangdianbiao.get("wdno").asText(),
              wangdianbiao.get("splb").asText(),
              wangdianbiao.get("scqy").asText(),
              wangdianbiao.get("fwlb").asText(),
              wangdianbiao.get("sjwdno").asText(),
              wangdianbiao.get("sjwdmc").asText(),
              wangdianbiao.get("sjwdxtbh").asText(),
              wangdianbiao.get("cxqyfw").asText(),
              wangdianbiao.get("stat").asText(),
              wangdianbiao.get("czren").asText(),
              wangdianbiao.get("czrmc").asText(),
              wangdianbiao.get("czsj").asText(),
              wangdianbiao.get("zhczsj").asText(),
              wangdianbiao.get("cxqyfwbh").asText(),
              wangdianbiao.get("sfxyzq").asText(),
              wangdianbiao.get("sfxyzqbak").asText(),
              wangdianbiao.get("seno").asText(),
              wangdianbiao.get("leix").asText(),
              wangdianbiao.get("wdmc").asText(),
              wangdianbiao.get("spmc").asText(),
              wangdianbiao.get("scqymc").asText(),
              caoZuoType))
          } catch {
            case e: Exception => logger.error("WangDianSjdwmxFunction抓取数据异常->" + e.getMessage)
          }

       // }
      }
    }
  }
}
