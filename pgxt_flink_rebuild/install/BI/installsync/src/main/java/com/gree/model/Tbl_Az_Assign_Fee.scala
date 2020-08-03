package com.gree.model

case class Tbl_Az_Assign_Fee(
                            id	: String,	//	主键,
                            created_by	: String,
                            created_date	: String,
                            last_modified_by	: String,
                            last_modified_date	: String,
                            otherfee: String,
                            totalfee: String,
                            ajia: String,
                            jcguan: String,
                            kqkg: String,
                            gkzy: String,
                            yccxqk: String,
                            flbz: String,
                            pgguid: String,
                            ts : String ,//数据日志时间戳
                            table : String //表名称
                          )
