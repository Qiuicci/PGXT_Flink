package com.gree.model

case class Tbl_Az_Assign_Lc_Ls(
                                pgguid	: String,		//	派工信息单主键,
                                created_by	: String,		//
                                created_date	: String,		//
                                last_modified_by	: String,		//
                                last_modified_date	: String,		//
                                pgid	: String,		//	派工序号,
                                yhmc	: String,		//	姓名,
                                yddh	: String,		//	移动电话,
                                yddh2	: String,		//	移动电话2,
                                quhao	: String,		//	区号,
                                dhhm	: String,		//	电话号码,
                                fjhm	: String,		//	分机号,
                                email	: String,		//	电子邮箱,
                                sfen	: String,		//	省份,
                                cshi	: String,		//	市,
                                xian	: String,		//	县,
                                xzhen	: String,		//	街道办/乡镇,
                                dizi	: String,		//	地址,
                                xxqd	: String,		//	信息渠道,
                                xxly	: String,		//	信息来源,
                                xxlb	: String,		//	信息类别,
                                beiz	: String,		//	备注,
                                yhsx	: String,		//	用户属性,
                                yxji	: String,		//	优先级,
                                gdhao	: String,		//	归档号,
                                spid	: String,		//	商品类别序号,
                                spmc	: String,		//	品类名称,
                                azren	: String,		//	安装人,
                                azrenid	: String,		//	安装人ID,
                                azwdxtbh	: String,		//	安装网点系统编号,
                                azwdno	: String,		//	安装网点,
                                azwdmc	: String,		//	安装网点名称,
                                jspgwdno	: String,		//	接收派工网点,
                                jspgwdmc	: String,		//	接收派工网点名称,
                                jspgwdxtbh	: String,		//	接收派工网点系统编号,
                                jspgwdsj	: String,		//	接收派工时间,
                                zxha	: String,		//	座席号,
                                ssqy	: String,		//	所属区域,
                                qqlyno	: String,		//	来源系统,
                                qqlymc	: String,		//	信息方式名称,
                                qqlyxh	: String,		//	来源序号,
                                qqlyzj	: String,		//	来源系统主信息主键,
                                bjustat	: String,		//	检测北交云扫描状态,
                                yhqwkssj	: String,		//	用户期望上门开始时间,
                                yhqwjssj	: String,		//	用户期望上门结束时间,
                                stat	: String,		//	派工状态,
                                gpsdzxx	: String,		//	GPS地址信息,
                                cjren	: String,		//	创建人,
                                cjrmc	: String,		//	创建人姓名,
                                cjdt	: String,		//	创建时间,
                                cjwdno	: String,		//	创建网点,
                                cjwdxtbh	: String,		//	创建网点系统,
                                zjczren	: String,		//	最近操作人,
                                zjczwd	: String,		//	最近操作网点,
                                zjczwdxtbh	: String,		//	系统网点编号,
                                zjczsj	: String,		//	最近操作时间,
                                xslx	: String,		//	销售类型,
                                lcid	: String,		//	流程序号,
                                djlxno	: String,		//	单据类型编号,
                                yyazsj	: String,		//	预约安装时间,
                                sfwcps	: String,		//	是否完成配送,
                                xsdh	: String,		//	销售单号,
                                gcbh	: String,		//	工程编号,
                                gcmc	: String,		//	工程名称,
                                azsl	: String,		//	安装总数量,
                                wwsl	: String,		//	未完成数量,
                                lxren	: String,		//	联系人,
                                xsdwno	: String,		//	销售单位编号,
                                xswdmc	: String,		//	销售单位名称,
                                xsdwxtbh	: String,		//	销售单位系统网点编号,
                                fphm	: String,		//	发票号码,
                                gmsj	: String,		//	购买日期,
                                kqbh	: String,		//	跨区编号,
                                xsorsh	: String,		//	数据录入属性,
                                dqjdsj	: String,		//	当前节点时间,
                                syjd	: String,		//	上一节点,
                                dqjd	: String,		//	当前节点,
                                jindu	: String,		//	经度,
                                weidu	: String,		//	纬度,
                                shsj	: String,		//	送货时间,
                                sfygllc	: String,		//	是否有关联流程,
                                xslxid	: String,		//	销售类型id,
                                yhsxid	: String,		//	用户属性id,
                                xxlbid	: String,		//	信息类别id,
                                xxlyid	: String,		//	信息来源id,
                                sfenid	: String,		//	省份id,
                                cshiid	: String,		//	市id,
                                xianid	: String,		//	县id,
                                xzhenid	: String,		//	乡镇id,
                                wcsj:String,
                                retailsign:String,//新零售标识
                                servicewdmc:String,//分销网点名称
                                shopname:String,//店铺名称
                                orderphone:String,//下单人电话
                                extendfiled1:String,//扩展备用字段1
                                extendfiled2:String,//扩展备用字段2
                                extendfiled3:String,//扩展备用字段3
                                extendfiled4:String,//扩展备用字段4
                                servicewdno:String,//分销网点编号
                                shopno:String,//销售网点信息
                                extendfiled5:String,//扩展备用字段5
                                organizationaddress:String,//提货仓库地址
                                organizationname:String,//提货仓库名称
                                czzhuangtai: String ,//操作状态,数据是插入，更新或者删除
                                 ts : String ,//数据日志时间戳
                                 table : String //表名称
                               )
