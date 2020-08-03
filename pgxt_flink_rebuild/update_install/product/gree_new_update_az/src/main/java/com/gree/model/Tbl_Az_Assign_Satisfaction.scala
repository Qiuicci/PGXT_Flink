package com.gree.model

case class Tbl_Az_Assign_Satisfaction (
    id : String,	// varchar(255) NOT NULL,
    created_by: String,	//  varchar(255) DEFAULT NULL,
    created_date: String,	//  datetime DEFAULT NULL,
    last_modified_by: String,	//  varchar(255) DEFAULT NULL,
    last_modified_date: String,	//  datetime DEFAULT NULL,
    pgguid: String,	//  varchar(40) DEFAULT NULL COMMENT '主键id',
    pjly: String,	//  varchar(15) DEFAULT NULL COMMENT '回访；用户短信自评',
    pjnr : String,	// varchar(50) DEFAULT NULL COMMENT '满意、一般、不满意',
    hfren : String,	// varchar(30) DEFAULT NULL COMMENT '回访人',
    hfwdmc: String,	//  varchar(30) DEFAULT NULL COMMENT '回访人网点名称',
    hfwdno: String,	//  varchar(30) DEFAULT NULL COMMENT '回访人网点编号',
    hfsj: String,	//  datetime DEFAULT NULL COMMENT '回访时间',
    bmylx: String,	//  varchar(50) DEFAULT NULL COMMENT '不满意类型',
    bmybeiz: String,	//  varchar(500) DEFAULT NULL COMMENT '不满意备注',
    bmysj: String,	//  datetime DEFAULT NULL COMMENT '不满意时间',
    splb : String,	// bigint(64) DEFAULT NULL COMMENT '商品大类',
    mydlx: String,	//  int(11) DEFAULT '0' COMMENT '类型：预约；改约',
    sxlx: String	//  int(11) DEFAULT '0' COMMENT '类型：首次；24内；24外',

)
