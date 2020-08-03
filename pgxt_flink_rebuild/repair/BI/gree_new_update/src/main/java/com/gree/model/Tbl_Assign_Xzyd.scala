package com.gree.model

case class Tbl_Assign_Xzyd(
                            xzid:String,// '主键',
                           created_by :String ,// NULL,
                           created_date :String,// NULL,
                           last_modified_by: String ,// NULL,
                           last_modified_date: String  ,// NULL,
                           pgid: String ,// '0' COMMENT '维修单id',
                           czren: String ,// NULL COMMENT '操作人',
                           czsj: String ,// NULL COMMENT '操作时间',
                           wdno: String ,// NULL COMMENT '操作网点',
                           cshu: String ,// '0' COMMENT '第几次',
                           xzyq :String ,// NULL COMMENT '新增要求内容',
                           xzyqlb: String ,// NULL COMMENT '新增要求类别',
                           ydbz: String ,// '0' COMMENT '是否已被阅读',
                           ydsj:String  ,// NULL COMMENT '阅读时间',
                           ydren:String ,// NULL COMMENT '阅读人账号',
                           ydrmc: String ,// NULL COMMENT '阅读人名称',
                           ydwd: String ,// NULL COMMENT '阅读人所在网点编号',
                           ydwdmc:String, // NULL COMMENT '阅读人所在网点名称'
                          caozuoType:String
 )
