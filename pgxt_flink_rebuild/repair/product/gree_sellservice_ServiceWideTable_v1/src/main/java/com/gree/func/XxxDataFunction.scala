package com.gree.func

import java.util

import com.gree.constant.Constant
import com.gree.model.{Tbl_Assign_Model, Tbl_Assign_Xzyd}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class XxxDataFunction(modeldata:OutputTag[Tbl_Assign_Model]) extends ProcessFunction[ObjectNode,Tbl_Assign_Xzyd]{
  val logger: Logger = LoggerFactory.getLogger(XxxDataFunction.super.getClass)
  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, Tbl_Assign_Xzyd]#Context, out: Collector[Tbl_Assign_Xzyd]): Unit = {


    //data中可能有多个对象，遍历取出
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
    val caoZuoType = value.get("value").get("type").asText()
    val ts = value.get("value").get("ts").asText()
    val table = value.get("value").get("table").asText()

    if ("DELETE".equals(caoZuoType)){
      logger.info("XxxDataFunction此数据为DELETE不做处理")
    }else {
    while(data.hasNext) {
      val xxxbiao: JsonNode = data.next()
      //需要同步的数据
      try {
        out.collect(Tbl_Assign_Xzyd(
          xxxbiao.get("xzid").asText(), //:String,// '主键',
          xxxbiao.get("created_by").asText(), // :String ,// NULL,
          xxxbiao.get("created_date").asText(), // :String,// NULL,
          xxxbiao.get("last_modified_by").asText(), //: String ,// NULL,
          xxxbiao.get("last_modified_date").asText(), //: String  ,// NULL,
          xxxbiao.get("pgid").asText(), // : String ,// '0' COMMENT '维修单id',
          xxxbiao.get("czren").asText(), // : String ,// NULL COMMENT '操作人',
          xxxbiao.get("czsj").asText(), // : String ,// NULL COMMENT '操作时间',
          xxxbiao.get("wdno").asText(), // : String ,// NULL COMMENT '操作网点',
          xxxbiao.get("cshu").asText(), // : String ,// '0' COMMENT '第几次',
          xxxbiao.get("xzyq").asText(), //  :String ,// NULL COMMENT '新增要求内容',
          xxxbiao.get("xzyqlb").asText(), // : String ,// NULL COMMENT '新增要求类别',
          xxxbiao.get("ydbz").asText(), // : String ,// '0' COMMENT '是否已被阅读',
          xxxbiao.get("ydsj").asText(), // :String  ,// NULL COMMENT '阅读时间',
          xxxbiao.get("ydren").asText(), // :String ,// NULL COMMENT '阅读人账号',
          xxxbiao.get("ydrmc").asText(), // : String ,// NULL COMMENT '阅读人名称',
          xxxbiao.get("ydwd").asText(), // : String ,// NULL COMMENT '阅读人所在网点编号',
          xxxbiao.get("ydwdmc").asText(), // :String, // NULL COMMENT '阅读人所在网点名称'
          caoZuoType,ts,table
        ))

        Thread.sleep(60)

        //需要进行逻辑计算的数据
        ctx.output[Tbl_Assign_Model](modeldata, Tbl_Assign_Model(
          Constant.TBL_ASSIGN_XZYD_INDEX,
          Constant.TBL_ASSIGN_XZYD_QUERY_ID,
          xxxbiao.get("xzid").asText(),// '主键',
          xxxbiao.get("pgid").asText(),
          table,ts
        ))
      } catch {
        case e: Exception => logger.error("维修看板XxxDataFunction抓取数据异常->" + e.getMessage)
      }
    }
    }
  }
}
