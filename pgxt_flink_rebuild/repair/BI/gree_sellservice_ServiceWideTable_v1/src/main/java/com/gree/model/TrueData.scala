package com.gree.model

case class TrueData(
                     id:String, //主键
                     pgid:String,//工单id
                     table:String, //数据来源表名
                     ts:String //数据时间戳
                   )
