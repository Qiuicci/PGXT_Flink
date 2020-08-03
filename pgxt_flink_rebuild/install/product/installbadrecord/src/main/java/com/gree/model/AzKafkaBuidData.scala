package com.gree.model

case class AzKafkaBuidData(
                             id : String ,//表的id名称
                             pgguid : String ,//安装业务保留的pgguid
                             table : String ,//所在的表名称
                             ts : Long  //时间位点
                          )
