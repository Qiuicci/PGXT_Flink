package com.gree.evaluate

import java.util

import com.gree.model.{TblWangdianSjdwmx, WangdianLevel}
import com.gree.util.SplitUtil
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


class QuanXianLevelPanDuanFunction(wangdianLevelData : OutputTag[WangdianLevel]) extends ProcessFunction[(String,TblWangdianSjdwmx),WangdianLevel]{
  override def processElement(value: (String, TblWangdianSjdwmx), ctx: ProcessFunction[(String, TblWangdianSjdwmx), WangdianLevel]#Context, out: Collector[WangdianLevel]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(QuanXianLevelPanDuanFunction.super.getClass)

    //用cxqyfw字段8个一组拆分进行详细逻辑判断


    val length: Int = (value._2.cxqyfwbh.length/8)

    var first : String = "null"
    var second : String = "null"
    var third : String = "null"
    var fourth : String = "null"
    var fifth : String = "null"
    var sixth : String = "null"
    var seventh : String = "null"

    val result: util.ArrayList[String] = SplitUtil.SplitFunctionFor8(value._2.cxqyfwbh)

    for (i <- 0 until length){
        if(i==0){
          first = result.get(0)
          logger.info("first为"+first)
        }
        if(i==1){
          second = result.get(1)
          logger.info("second为"+second)
        }
        if(i==2){
          third = result.get(2)
          logger.info("third为"+third)
        }
        if(i==3){
          fourth = result.get(3)
          logger.info("fourth为"+fourth)
        }
        if(i==4){
          fifth = result.get(4)
          logger.info("fifth为"+fifth)
        }
        if(i==5){
          sixth = result.get(5)
          logger.info("sixth为"+sixth)
        }
      if(i==6){
        seventh = result.get(6)
        logger.info("seventh为"+seventh)
      }
    }

    out.collect(WangdianLevel(value._1,value._2.created_by,value._2.created_date,value._2.last_modified_by,value._2.last_modified_date,
      value._2.xhid,value._2.xtwdbh,value._2.wdno,value._2.splb,value._2.scqy,value._2.fwlb,value._2.sjwdno,value._2.sjwdmc,
      value._2.sjwdxtbh,value._2.cxqyfw,value._2.stat,value._2.czren,value._2.czrmc,value._2.czsj,value._2.zhczsj,value._2.cxqyfwbh,
      value._2.sfxyzq,value._2.sfxyzqbak,value._2.seno,value._2.leix,value._2.wdmc,value._2.spmc,value._2.scqymc,first,second,third,
      fourth,fifth,sixth,seventh))

  }
}
