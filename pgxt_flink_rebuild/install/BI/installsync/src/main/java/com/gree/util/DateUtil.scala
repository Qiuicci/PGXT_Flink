package com.gree.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  def getDateFormatTime(date:Date) ={
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentDate: String = simpleDateFormat.format(date)
    currentDate
  }

  def getDateFormatDay(date:Date) ={
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val currentDate: String = simpleDateFormat.format(date)
    currentDate
  }
  def getDateFormatSecDay(date:Date) ={
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val currentDate: String = simpleDateFormat.format(date)
    currentDate
  }
}