package com.gree.model

case class Tbl_Assign_Appointment(
                                   id: String,
                                   created_by: String,
                                   created_date: String,
                                   last_modified_by: String,
                                   last_modified_date: String,
                                   beiz: String, //备注
                                   czren: String, //操作人
                                   czsj: String, //操作时间
                                   jssj: String, //用户预约时间：结束时间
                                   kssj: String, //用户预约时间:开始时间
                                   leix: String, //类型
                                   pgid: String, //派工pgguid
                                   reason: String, //延误类型
                                   zaozuoType: String //操作类型
                                 )
