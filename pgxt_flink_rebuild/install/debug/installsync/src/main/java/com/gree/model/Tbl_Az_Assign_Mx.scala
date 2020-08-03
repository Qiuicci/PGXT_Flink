package com.gree.model

case class Tbl_Az_Assign_Mx(
    pgmxid : String,	//VARCHAR ( 255 ) NOT NULL,
    created_by : String,	// VARCHAR ( 255 ) DEFAULT NULL,
    created_date : String,	// datetime DEFAULT NULL,
    `last_modified_by` : String,	// VARCHAR ( 255 ) DEFAULT NULL,
    `last_modified_date` : String,	// datetime DEFAULT NULL,
    `pgguid`  : String,	//VARCHAR ( 40 ) DEFAULT NULL COMMENT '派工主键',
  spid : String,	// BIGINT ( 75 ) DEFAULT '0' COMMENT '产品大类序号',
  spmc  : String,	//VARCHAR ( 73 ) DEFAULT NULL COMMENT '产品大类名称',
  xlid  : String,	//BIGINT ( 73 ) DEFAULT '0' COMMENT '小类序号',
  xlmc : String,	//VARCHAR ( 60 ) DEFAULT NULL COMMENT '小类名称',
  xiid : String,	// BIGINT ( 74 ) DEFAULT '0' COMMENT '系列序号',
  ximc  : String,	//VARCHAR ( 80 ) DEFAULT NULL COMMENT '系列名称',
  jxmc: String,// VARCHAR ( 200 ) DEFAULT NULL COMMENT '机型名称',
  jxno : String,//VARCHAR ( 40 ) DEFAULT NULL COMMENT '机型编号',
  czren: String,// VARCHAR ( 80 ) DEFAULT NULL COMMENT '操作人',
  czsj : String,//datetime DEFAULT NULL COMMENT '操作时间',
  czwd : String,//VARCHAR ( 8 ) DEFAULT NULL COMMENT '操作网点',
  njtm : String,//VARCHAR ( 22 ) DEFAULT NULL COMMENT '内机条码',
  wjtm : String,//VARCHAR ( 22 ) DEFAULT NULL COMMENT '外机条码',
  beiz : String,//VARCHAR ( 600 ) DEFAULT NULL COMMENT '明细备注',
  shul : String,//INT ( 11 ) DEFAULT '0' COMMENT '数量',
  cjdt : String,//datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  jiage: String,// DECIMAL ( 10, 0 ) DEFAULT '0' COMMENT '价格',
  danw : String,//VARCHAR ( 6 ) DEFAULT NULL COMMENT '单位',
  wldm : String,//VARCHAR ( 47 ) DEFAULT NULL COMMENT '物料代码',
  njtm2: String,// VARCHAR ( 22 ) DEFAULT NULL COMMENT '内机条码2',
  wjsl : String,//INT ( 11 ) DEFAULT '1' COMMENT '外机数量',
  njsl : String,//INT ( 11 ) DEFAULT '1' COMMENT '内机数量',
  wwsl : String,//INT ( 11 ) DEFAULT '0' COMMENT '未完成数量',
    ts : String ,//数据日志时间戳
    table : String //表名称

 // ) ENGINE = INNODB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '（新）安装工单明细表';

                            )




