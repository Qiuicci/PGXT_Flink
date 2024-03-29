package com.gree.model

case class AzChanPinMingXiKuanBiaoData(
                                 //安装主表
                                 pgguid: String, //'派工信息单主键'
                                 created_by: String,
                                 created_date: String,
                                 last_modified_by: String,
                                 last_modified_date: String,
                                 pgid: String, //'派工序号'
                                 yhmc: String, //'姓名'
                                 yddh: String, //'移动电话'
                                 yddh2: String, //'移动电话2'
                                 quhao: String, //'区号'
                                 dhhm: String, //'电话号码'
                                 fjhm: String, //'分机号'
                                 email: String, //'电子邮箱'
                                 sfen: String, //'省份'
                                 cshi: String, //'市'
                                 xian: String, //'县'
                                 xzhen: String, //'街道办/乡镇'
                                 dizi: String, //'地址'
                                 xxqd: String, //'信息渠道'
                                 xxly: String, //'信息来源'
                                 xxlb: String, //'信息类别'
                                 beiz: String, //'备注'
                                 yhsx: String, //'用户属性'
                                 yxji: String, //'优先级'
                                 gdhao: String, //'归档号'
                                 spid: String, //'商品类别序号'
                                 spmc: String, //'品类名称'
                                 azren: String, //'安装人'
                                 azrenid: String, //'安装人ID'
                                 azwdxtbh: String, //'安装网点系统编号'
                                 azwdno: String, //'安装网点'
                                 azwdmc: String, //'安装网点名称'
                                 jspgwdno: String, //'接收派工网点'
                                 jspgwdmc: String, //'接收派工网点名称'
                                 jspgwdxtbh: String, //'接收派工网点系统编号'
                                 jspgwdsj: String, //'接收派工时间'
                                 zxha: String, //'座席号'
                                 ssqy: String, //'所属区域'
                                 qqlyno: String, //'来源系统'
                                 qqlymc: String, //'信息方式名称'
                                 qqlyxh: String, //'来源序号'
                                 qqlyzj: String, //'来源系统主信息主键'
                                 bjustat: String, //'检测北交云扫描状态'
                                 yhqwkssj: String, //'用户期望上门开始时间'
                                 yhqwjssj: String, //'用户期望上门结束时间'
                                 stat: String, //'派工状态'
                                 gpsdzxx: String, //'GPS地址信息'
                                 cjren: String, //'创建人'
                                 cjrmc: String, //'创建人姓名'
                                 cjdt: String, //'创建时间'
                                 cjwdno: String, //'创建网点'
                                 cjwdxtbh: String, //'创建网点系统'
                                 zjczren: String, //'最近操作人'
                                 zjczwd: String, //'最近操作网点'
                                 zjczwdxtbh: String, //'系统网点编号'
                                 zjczsj: String, //'最近操作时间'
                                 xslx: String, //'销售类型'
                                 lcid: String, //'流程序号'
                                 djlxno: String, //'单据类型编号'
                                 yyazsj: String, //'预约安装时间'
                                 sfwcps: String, //'是否完成配送'
                                 xsdh: String, //'销售单号'
                                 gcbh: String, //'工程编号'
                                 gcmc: String, //'工程名称'
                                 azsl: String, //'安装总数量'
                                 wwsl: String, //'未完成数量'
                                 lxren: String, //'联系人'
                                 xsdwno: String, //'销售单位编号'
                                 xswdmc: String, //'销售单位名称'
                                 xsdwxtbh: String, //'销售单位系统网点编号'
                                 fphm: String, //'发票号码'
                                 gmsj: String, //'购买日期'
                                 kqbh: String, //'跨区编号'
                                 xsorsh: String, //'数据录入属性'
                                 dqjdsj: String, //'当前节点时间'
                                 syjd: String, //'上一节点'
                                 dqjd: String, //'当前节点'
                                 jindu: String, //'经度'
                                 weidu: String, //'纬度'
                                 shsj: String, //'送货时间'
                                 sfygllc: String, //'是否有关联流程'
                                 xslxid: String, //'销售类型id'
                                 yhsxid: String, //'用户属性id'
                                 xxlbid: String, //'信息类别id'
                                 xxlyid: String, //'信息来源id'
                                 sfenid: String, //'省份id'
                                 cshiid: String, //'市id'
                                 xianid: String, //'县id'
                                 xzhenid: String, //'乡镇id'
                                 tblAzAssignFkmxFkid: String, //'主键'
                                 tblAzAssignFkmxFklb: String, //'反馈类别'
                                 tblAzAssignFkmxFkjg: String, //'反馈结果'
                                 tblAzAssignFkmxFknr: String, //'反馈内容'
                                 tblAzAssignFkmxFkren: String, //'反馈人'
                                 tblAzAssignFkmxFkrenmc: String, //'反馈人姓名'
                                 tblAzAssignFkmxFksj: String, //'反馈时间'
                                 tblAzAssignFkmxXtwdbh: String, //'系统网点编号'
                                 tblAzAssignFkmxWdno: String, //'反馈网点'
                                 tblAzAssignFkmxWdmc: String, //'反馈网点名称'
                                 tblAzAssignFkmxFkmxcjdt: String, //'创建时间'
                                 tblAzAssignAppointmentId: String, //'主键'
                                 tblAzAssignAppointmentKssj: String, //'用户预约时间:开始时间'
                                 tblAzAssignAppointmentJssj: String, //'用户预约时间：结束时间'
                                 tblAzAssignAppointmentCzren: String, //'操作人'
                                 tblAzAssignAppointmentCzsj: String, //'操作时间'
                                 tblAzAssignAppointmentLeix: String, //'类型'
                                 tblAzAssignAppointmentReason: String, //'延误类型'
                                 tblAzAssignAppointmentBeiz: String, //备注
                                 yqwangongtime: String, // 要求完工时间(有预约时间:取最新开始时间+24h & 无预约时间:取安装主表创建时间+24小时)
                                 azChanPinMingXi:String,
                                 shoucipaigongtime:String,
                                 shijiwangongtime:String,
                                 level1:String,
                                 level2:String,
                                 level3:String,
                                 level4:String,
                                 level5:String,
                                 level6:String,
                                 level7:String,
                                 level8:String,
                                 level9:String,
                                 level10:String,
                                 level11:String,
                                 level12:String,
                                 level13:String,
                                 level14:String
                               )
