package com.gree.model

case class AzKafkaBuidData(
                             id : String = null,//表的id名称
                             pgguid : String = null,//安装业务保留的pgguid
                             table : String = null,//所在的表名称
                             ts : String = null //时间位点
                          )
