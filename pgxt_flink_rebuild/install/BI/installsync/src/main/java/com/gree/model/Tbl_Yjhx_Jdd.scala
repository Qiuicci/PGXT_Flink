package com.gree.model

case class Tbl_Yjhx_Jdd(
                       id	: String,	//	主键,
                       created_by	: String,
                       created_date	: String,
                       last_modified_by	: String,
                       last_modified_date	: String,
                       oldMachineBrand: String,
                        oldMachineType: String,
                        brandFlag: String,
                        typeFlag: String,
                        realBrand: String,
                        realType: String,
                        machineIntegrity: String,
                        tempBarcode: String,
                        tempBarcodeImg: String,
                        identifyResult: String,
                        hxjddid: String,
                        pgguid: String,
                        xsdh: String,
                        cpps: String,
                       ts : String ,//数据日志时间戳
                       table : String //表名称
                     )
