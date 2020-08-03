package com.gree.model

case class Tbl_Az_Assign_Platform(
                                id	: String,	//	主键,
                                created_by	: String,
                                created_date	: String,
                                last_modified_by	: String,
                                last_modified_date	: String,
                                parentbizorderid : String,
                                bizorderid : String,
                                pgguid : String,
                                yywm : String,
                                xdsj : String,
                                ts : String ,//数据日志时间戳
                                table : String //表名称
                              )
