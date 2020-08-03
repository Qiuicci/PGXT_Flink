package com.gree.model

case class Tbl_Az_Assign_Fkmx(
                               fkid	: String,	//	主键,
                               created_by	: String,	//
                               created_date	: String,	//
                               last_modified_by	: String,	//
                               last_modified_date	: String,	//
                               fklb	: String,	//	反馈类别,
                               fkjg	: String,	//	反馈结果,
                               fknr	: String,	//	反馈内容,
                               fkren	: String,	//	反馈人,
                               fkrenmc	: String,	//	反馈人姓名,
                               fksj	: String,	//	反馈时间,
                               xtwdbh	: String,	//	系统网点编号,
                               wdno	: String,	//	反馈网点,
                               wdmc	: String,	//	反馈网点名称,
                               pgguid	: String,	//	序号主键,
                               cjdt	: String,	//	创建时间,
                               czzhuangtai: String //操作状态,数据是插入，更新或者删除

                             )
