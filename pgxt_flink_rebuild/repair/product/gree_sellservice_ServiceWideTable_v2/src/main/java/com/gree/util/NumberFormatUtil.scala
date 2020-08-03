package com.gree.util

import java.lang.{Double => Jdouble, Long => Jlong}
import java.text.SimpleDateFormat

import org.slf4j.{Logger, LoggerFactory}

class NumberFormatUtil {

  val logger: Logger = LoggerFactory.getLogger(NumberFormatUtil.super.getClass)
  def panduanLong(longNum:String):Long={
    val num:Long =0
    if ("null".equals(longNum)||longNum==null||"".equals(longNum)){
      num
    }else{
      try {
        Jlong.valueOf(longNum)
      } catch {
        case e:Exception =>{logger.error("gree_new_installationboardpanduanLong转换异常->"+e.getMessage)
        0}
      }
    }
  }

  def  panduanInt(intNum:String):Int={
    val num:Int =0
    if ("null".equals(intNum)||intNum==null||"".equals(intNum)){
      num
    }else{
      try {
        Integer.valueOf(intNum)
      } catch {
        case e:Exception =>{logger.error("gree_new_installationboardpanduanInt转换异常->"+e.getMessage)
        0}
      }
    }
  }

  def panduanDouble(doubleNum:String):Double= {
    val num:Double = 0.0
    if("null".equals(doubleNum)||doubleNum==null||"".equals(doubleNum)){
      num
    }else{
      try {
        Jdouble.valueOf(doubleNum)
      } catch {
        case e:Exception =>{logger.error("gree_new_installationboardpanduanDouble转换异常->")
        0.0}
      }
    }
  }

  def panduanDate(jssjDate:String):String= {
    val num:String = "1970-01-01 00:00:00"
    if("null".equals(jssjDate)||jssjDate==null||"".equals(jssjDate)){
      num
    }else{
      jssjDate
    }
  }

  def getDateLong(date:String):Long= {
    val num:String = "1970-01-01 00:00:00"
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if("null".equals(date)||date==null||"".equals(date)){
      simpleDateFormat.parse(num).getTime
    }else{
      try {
        simpleDateFormat.parse(date).getTime
      } catch {
        case e:Exception =>{logger.error("gree_new_installationboard时间转换异常->"+e.getMessage)
          simpleDateFormat.parse(num).getTime
        }
      }
    }
  }

  def  panduanInt1(intNum:String):Int={
    val num:Int =1
    if ("null".equals(intNum)||intNum==null||"".equals(intNum)){
      num
    }else{
      try {
        Integer.valueOf(intNum)
      } catch {
        case e:Exception =>{logger.error("gree_new_installationboardpanduanInt1转换异常->"+e.getMessage)
          1}
      }
    }
  }
  def panduanNull(str:String):Any={
    if ("null".equals(str)||"".equals(str)){
      null
    }else{
      str
    }
  }

}
