package com.gree.model

case class Tbl_Trade_New_For_Old(
                              id	: String,	//	主键,
                              created_by	: String,
                              created_date	: String,
                              last_modified_by	: String,
                              last_modified_date	: String,
                              oldMachineBrand: String,
                              oldMachineType: String,
                              oldMachineNum: String,
                              connector: String,
                              connectWay: String,
                              hxmxid: String,
                              pgguid: String,
                              xsdh: String,
                              orderitemid: String,
                              lcid: String,
                              ts : String ,//数据日志时间戳
                              table : String //表名称
                            )
