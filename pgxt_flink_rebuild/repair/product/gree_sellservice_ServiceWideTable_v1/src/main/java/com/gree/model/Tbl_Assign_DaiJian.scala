package com.gree.model

case class Tbl_Assign_DaiJian(
                           id:String,
                           created_by :String ,// NULL,
                           created_date :String,// NULL,
                           last_modified_by: String ,// NULL,
                           last_modified_date: String  ,// NULL,
                           pjsqbh:String,
                           pjsqsj:String,
                           pjwlbm:String,
                           pjwlmc:String,
                           pjwlsl:String,
                           djquyunum:String,
                           djxsgsnum:String,
                           djwdnum:String,
                           pgid:String,
                           djwd:String,
                           djwdmc:String,
                           djsj:String,
                           splb:String,
                           cjdt:String,
                           thwlbm:String,
                           thwlmc:String,
                           thbmxsgsnum:String,
                           thbmqynum:String,
                           thbmwdnum:String,
                           pjxtflag:String,
                           caozuoType:String,
                           ts:String, //数据时间戳
                           table: String //数据来源表名称
                           )
