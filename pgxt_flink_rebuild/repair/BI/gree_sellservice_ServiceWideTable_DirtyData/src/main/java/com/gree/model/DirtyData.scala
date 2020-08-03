package com.gree.model

case class DirtyData (
                       id:String, //主键
                       pgid:String,//工单id
                       table:String, //数据来源表名
                       ts:String, //数据时间戳
                       state:Long , //状态0标示正常丢弃，1表示ES丢数据
                       describe:String //脏数据描述
                     )
