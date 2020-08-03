package com.gree.model

case class Tbl_Assign_Model (
                            index:String,//对应es中索引名称
                            query_name:String,//es中查询id名称
                            id:String,//数据主键
                            pgid:String,//派工单id
                            table:String,//数据来源表名
                            ts:String//数据时间戳
                            )
