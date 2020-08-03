package com.gree.func

import java.util

import com.gree.constant.Constant
import com.gree.model.{Tbl_Assign_DaiJian, Tbl_Assign_Model}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class DaiJianBiaoDataFunction(modeldata:OutputTag[Tbl_Assign_Model]) extends ProcessFunction[ObjectNode,Tbl_Assign_DaiJian]{

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Assign_DaiJian]#Context, out: Collector[Tbl_Assign_DaiJian]): Unit = {
    //data中可能有多个对象，遍历取出
    val logger: Logger = LoggerFactory.getLogger(DaiJianBiaoDataFunction.super.getClass)
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()


    if ("DELETE".equals(caoZuoType)){
      logger.info("DaiJianBiaoDataFunction此数据为DELETE不做处理")
    }else {
    while (data.hasNext) {
      val daiJianBiao: JsonNode = data.next()
      //需要同步的数据
      try {
        out.collect(Tbl_Assign_DaiJian(
          daiJianBiao.get("id").asText(),
          daiJianBiao.get("created_by").asText(),
          daiJianBiao.get("created_date").asText(),
          daiJianBiao.get("last_modified_by").asText(),
          daiJianBiao.get("last_modified_date").asText(),
          daiJianBiao.get("pjsqbh").asText(),
          daiJianBiao.get("pjsqsj").asText(),
          daiJianBiao.get("pjwlbm").asText(),
          daiJianBiao.get("pjwlmc").asText(),
          daiJianBiao.get("pjwlsl").asText(),
          daiJianBiao.get("djquyunum").asText(),
          daiJianBiao.get("djxsgsnum").asText(),
          daiJianBiao.get("djwdnum").asText(),
          daiJianBiao.get("pgid").asText(),
          daiJianBiao.get("djwd").asText(),
          daiJianBiao.get("djwdmc").asText(),
          daiJianBiao.get("djsj").asText(),
          daiJianBiao.get("splb").asText(),
          daiJianBiao.get("cjdt").asText(),
          daiJianBiao.get("thwlbm").asText(),
          daiJianBiao.get("thwlmc").asText(),
          daiJianBiao.get("thbmxsgsnum").asText(),
          daiJianBiao.get("thbmqynum").asText(),
          daiJianBiao.get("thbmwdnum").asText(),
          daiJianBiao.get("pjxtflag").asText(),
          caoZuoType,ts,table
        ))

        Thread.sleep(60)

        //需要进行逻辑计算的数据
        ctx.output[Tbl_Assign_Model](modeldata, Tbl_Assign_Model(
          Constant.TBL_ASSIGN_DAIJIAN_INDEX,
          Constant.TBL_ASSIGN_DAIJIAN_QUERY_ID,
          daiJianBiao.get("id").asText(),
          daiJianBiao.get("pgid").asText(),
          table,ts
        ))
      } catch {
        case e: Exception => logger.error("维修看板DaiJianBiaoDataFunction数据抓取出现异常->" + e.getMessage)
      }
    }
    }
  }

}
