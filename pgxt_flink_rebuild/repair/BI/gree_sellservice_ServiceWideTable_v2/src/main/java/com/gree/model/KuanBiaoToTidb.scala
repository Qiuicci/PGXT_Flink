package com.gree.model

case class KuanBiaoToTidb(
                           wxgd_cjdt:String,
                           wxgd_beiz:String,
                           wxgd_zbby:String,// 总部保养 ,
                           wxgd_xxlb:String,// as 信息类别,
                           wxgd_xxqd:String,// as 信息渠道,
                           wxgd_xxly:String,// 信息来源,
                           wxgd_wxwdno:String,// as 维修点编号,
                           wxgd_wxwdmc:String,// as 维修点名称,
                           wxgd_pgid:String,// 信息编号,
                           wxgd_yhsx:String,// 用户属性,
                           wxgd_quhao:String,// 长途区号,
                           wxgd_xian:String,// 县,
                           wxgd_stat:String,// 派工状态,
                           wxgd_cjren:String,// 信息创建人,
                          wxgd_xjwdsj:String,// 派工时间,
                          wxgd_cjwdno:String,// 创建网点,
                          wxgd_xjwdno:String,// 下级网点,
                          wxgd_ssqy:String,// 区域,
                          wxgd_yhmc:String,// 用户姓名,
                          wxgd_yddh:String,// 联系电话,
                          wxgd_sfen:String,// 省,
                          wxgd_cshi:String,// 市,
                           wxgd_spid:String,//品类id
                          wxgd_spmc:String,// 品类,
                          wxgd_wxren:String,// 维修人员,
                          wxgd_wxrenid:String,// 维修人id,
                           wxgd_yhqwsmsj:String,//用户期望上门开始时间
                           wxgd_qwsmjssj:String,//用户期望上门结束时间
                           wxgdyd_zxxs:Long,//子信息数
                           wxgdmx_spmc:String,// as 品类,
                          wxgdmx_xlmc:String,// as 小类名称,
                          wxgdmx_gmsj:String,// as 购买日期,
                          wxgdmx_jxmc:String,// as 机型名称,
                          wxgdmx_gzxx:String,// as 故障内容,
                          wxgdmyd_pjnr:String,// as 满意度结果,
                           wxgdyy_czsj:String,//预约操作时间
                           wxgdyy_kssj:String,//开始预约时间
                           wxgdyy_jssj:String,//结束预约时间
                           wxgdyy_yysj:String,//预约时间
                           bi_yqwgsj:String,//要求完工时间
//                           bi_timeout:String,//工单超时
                           wxgdfkmx_wg_fknr:String,// as 完工反馈内容,
                          wxgdfkmx_wg_fksj:String,// as 维修点完工反馈时间
                          wxgdfkmx_bh_fklb:String,// as 驳回类型,
                          wxgdfkmx_bh_fknr:String,// as 驳回内容,
                          wxgdfkmx_bh_fksj:String,// as 驳回时间,
                          wxgdfkmx_bh_fkwdmc:String,// as 驳回操作网点,
                           wxgdfkmx_dj:String//是否用户待件
//                           wxgddj_djwd:String,// 待件网点,
//                            wxgddj_djwdmc:String,// 待件网点名称,
//                            wxgddj_djsj:String,// 待件时间,
//                            wxgddj_pjsqbh:String,// 配件申请编号,
//                            wxgddj_pjsqsj:String,// 配件申请时间,
//                            wxgddj_pjwlbm:String,// 配件物料编码,
//                            wxgddj_pjwlmc:String,// 配件物料名称,
//                            wxgddj_pjwlsl:String,// 申请数量,
//                            wxgddj_djquyunum:String,// 区域库存,
//                            wxgddj_djxsgsnum:String,// 销售公司库存,
//                            wxgddj_djwdnum:String,// 网点库存,
//                            wxgddj_thwlbm:String,// 替换物料编码,
//                            wxgddj_thwlmc:String,// 替换物料名称,
//                            wxgddj_thbmxsgsnum:String,// 替换编码销售公司库存,
//                            wxgddj_thbmqynum:String,// 替换编码区域库存,
//                            wxgddj_thbmwdnum:String,// 替换编码网点库存,
//                            wxgdfb_zlfkbh:String,// as 质量反馈编号,
//                            wxgdfb_zlfksj:String,// as 质量反馈时间
//                           level1 : String, //权限结构点
//                           level2 : String,
//                           level3 : String,
//                           level4 : String,
//                           level5 : String,
//                           level6 : String,
//                           level7 : String,
//                           level8:String,
//                           level9:String,
//                           level10:String,
//                           level11:String,
//                           level12:String,
//                           level13:String,
//                           level14:String
                         )
