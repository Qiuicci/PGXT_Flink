package com.gree.model

case class Tbl_Assign_FeedBack(
                            id:String,
                            created_by: String,
                            created_date: String,
                            last_modified_by: String,
                            last_modified_date: String,
                            pgid:String,
                            zlfksj:String,
                            zlfkbh:String,
                            czren:String,
                            czsj:String,
                            caozuoType:String,
                            ts:String ,//数据时间戳
                            table: String //数据来源表名称
                            )
