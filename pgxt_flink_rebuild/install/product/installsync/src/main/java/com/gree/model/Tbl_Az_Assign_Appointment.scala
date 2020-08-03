package com.gree.model

case class Tbl_Az_Assign_Appointment(
                                       id	: String,	//	主键,
                                       created_by	: String,	//
                                       created_date	: String,	//
                                       last_modified_by	: String,	//
                                       last_modified_date	: String,	//
                                       kssj	: String,	//	用户预约时间:开始时间,
                                       jssj	: String,	//	用户预约时间：结束时间,
                                       czren	: String,	//	操作人,
                                       pgguid	: String,	//	派工GUID,
                                       czsj	: String,	//	操作时间,
                                       leix	: String,	//	类型,
                                       reason	: String,	//	延误类型,
                                       beiz	: String,//
                                       czzhuangtai: String, //操作状态,数据是插入，更新或者删除
                                       ts : String ,//数据日志时间戳
                                       table : String //表名称
                                     )



