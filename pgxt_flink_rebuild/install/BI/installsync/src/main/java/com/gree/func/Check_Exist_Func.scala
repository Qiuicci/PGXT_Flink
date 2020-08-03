package com.gree.func

import com.gree.model.AzKafkaBuidData
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 * 判断是否es已经存在数据
 * @param notExistRecord
 */
class Check_Exist_Func(notExistRecord: OutputTag[AzKafkaBuidData]) extends ProcessFunction[(AzKafkaBuidData,Boolean),AzKafkaBuidData]{
  override def processElement(input: (AzKafkaBuidData, Boolean), ctx: ProcessFunction[(AzKafkaBuidData, Boolean), AzKafkaBuidData]#Context, out: Collector[AzKafkaBuidData]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(Check_Exist_Func.super.getClass)
    if(input._2){
      out.collect( input._1 )
      logger.info("推送正常数据流 =>" +  input._1)
    }else{
      ctx.output(notExistRecord,input._1)
      logger.info("推送脏数据流 =>" +  input._1)

    }
  }
}
