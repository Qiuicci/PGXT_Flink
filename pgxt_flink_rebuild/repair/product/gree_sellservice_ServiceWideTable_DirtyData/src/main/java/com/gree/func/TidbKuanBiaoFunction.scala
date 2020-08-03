package com.gree.func

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.gree.constant.Constant
import com.gree.model.KuanBiaoToTidb
import com.gree.util.{ESTransportPoolUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.{Logger, LoggerFactory}


class TidbKuanBiaoFunction extends ProcessFunction[(String,Long),KuanBiaoToTidb]{
  override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), KuanBiaoToTidb]#Context, out: Collector[KuanBiaoToTidb]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(TidbKuanBiaoFunction.super.getClass)
    //连接ES
    val util1 = new NumberFormatUtil
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val client: TransportClient = ESTransportPoolUtil.getClient
    //初始化从主表查询出来的字段
    var wxgd_cjdt:String = "null"
    var wxgd_beiz:String = "null"
    var wxgd_zbby:String = "null"// 总部保养
    var wxgd_xxlb:String = "null"// as 信息类别,
    var wxgd_xxqd:String = "null"// as 信息渠道,
    var wxgd_xxly:String = "null"// 信息来源
    var wxgd_wxwdno:String = "null"// as 维修点编号
    var wxgd_wxwdmc:String = "null"// as 维修点名称,
    var wxgd_pgid:String = "null"// 信息编号
    var wxgd_yhsx:String = "null"// 用户属性
    var wxgd_quhao:String = "null"// 长途区号,
    var wxgd_xian:String = "null"// 县,
    var wxgd_stat:String = "null"// 派工状态,
    var wxgd_cjren:String = "null"// 信息创建人,
    var wxgd_xjwdsj:String = "null"// 派工时间,
    var wxgd_cjwdno:String = "null"// 创建网点,
    var wxgd_xjwdno:String = "null"// 下级网点,
    var wxgd_ssqy:String = "null"// 区域,
    var wxgd_yhmc:String = "null"// 用户姓名
    var wxgd_yddh:String = "null"// 联系电话,
    var wxgd_sfen:String = "null"// 省,
    var wxgd_cshi:String = "null"// 市
    var wxgd_spid:String = "null" // 品类id
    var wxgd_spmc:String = "null"// 品类
    var wxgd_wxren:String = "null"// 维修人员,
    var wxgd_wxrenid:String = "null"// 维修人id,
    var wxgd_yhqwsmsj:String = "null" //用户期望上门开始时间
    var wxgd_qwsmjssj:String = "null"//用户期望上门结束时间

    //根据主键pgid查询主表字段
    val zhuBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()
    val hits: SearchHits = zhuBiao.getHits
    if (hits.iterator().hasNext) {
      val hit = hits.iterator().next()
      logger.info("对应结果为->" + hit.getSourceAsString)
      try {
        wxgd_pgid = hit.getSourceAsMap.get("pgid").toString
        wxgd_beiz = hit.getSourceAsMap.get("beiz").toString
        wxgd_cjdt = hit.getSourceAsMap.get("cjdt").toString
        wxgd_cjren = hit.getSourceAsMap.get("cjren").toString
        wxgd_quhao = hit.getSourceAsMap.get("quhao").toString
        wxgd_qwsmjssj = hit.getSourceAsMap.get("qwsmjssj").toString
        wxgd_sfen = hit.getSourceAsMap.get("sfen").toString
        wxgd_spid = hit.getSourceAsMap.get("spid").toString
        wxgd_spmc = hit.getSourceAsMap.get("spmc").toString
        wxgd_ssqy = hit.getSourceAsMap.get("ssqy").toString
        wxgd_stat = hit.getSourceAsMap.get("stat").toString
        wxgd_wxren = hit.getSourceAsMap.get("wxren").toString
        wxgd_wxrenid = hit.getSourceAsMap.get("wxrenid").toString
        wxgd_wxwdmc = hit.getSourceAsMap.get("wxwdmc").toString
        wxgd_wxwdno = hit.getSourceAsMap.get("wxwdno").toString
        wxgd_xian = hit.getSourceAsMap.get("xian").toString
        wxgd_xjwdno = hit.getSourceAsMap.get("xjwdno").toString
        wxgd_xjwdsj = hit.getSourceAsMap.get("xjwdsj").toString
        wxgd_cjwdno = hit.getSourceAsMap.get("cjwdno").toString
        wxgd_xxlb = hit.getSourceAsMap.get("xxlb").toString
        wxgd_xxly = hit.getSourceAsMap.get("xxly").toString
        wxgd_xxqd = hit.getSourceAsMap.get("xxqd").toString
        wxgd_yddh = hit.getSourceAsMap.get("yddh").toString
        wxgd_yhmc = hit.getSourceAsMap.get("yhmc").toString
        wxgd_yhqwsmsj = hit.getSourceAsMap.get("yhqwsmsj").toString
        wxgd_yhsx = hit.getSourceAsMap.get("yhsx").toString
        wxgd_zbby = hit.getSourceAsMap.get("zbby").toString
        wxgd_cshi = hit.getSourceAsMap.get("cshi").toString

      } catch {
        case e: Exception => logger.info("TidbKuanBiaoFunction从主表查询异常->" + e.getMessage)
      }
    }

    //初始化子信息数量
    var wxgdyd_zxxs:Long = 0 //子信息数
    //根据主键从xzyd表中查询记录数
    val XZYDBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_XZYD_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()
    val xzydhits: SearchHits = XZYDBiao.getHits
    if (xzydhits.totalHits != 0){
      wxgdyd_zxxs = xzydhits.totalHits
    }

    //初始化满意度相关字段
    var wxgdmyd_pjnr:String = "null"// as 满意度结果,
    //根据pgid查询满意度表
    val mydBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_SATISFACTION_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("pgid",value._1))
        .must(QueryBuilders.termQuery("sxlx","0")))
      .get()
    val mydhits: SearchHits = mydBiao.getHits
    if ( mydhits.iterator().hasNext) {
      val hit = mydhits.iterator().next()
      try{
        wxgdmyd_pjnr = hit.getSourceAsMap.get("pjnr").toString
      }catch {
        case e:Exception => logger.error("TidbKuanBiaoFunction从满意度表查询异常->" + e.getMessage)
      }
    }

    //初始化明细表相关字段
    var wxgdmx_spmc:String = "null"// as 品类,
    var wxgdmx_xlmc:String = "null"// as 小类名称,
    var wxgdmx_gmsj:String = "null"// as 购买日期,
    var wxgdmx_jxmc:String = "null"// as 机型名称,
    var wxgdmx_gzxx:String = "null"// as 故障内容,

    //根据pgid查询明细表
    val mxBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_MX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .addSort("czsj",SortOrder.DESC)
      .setFrom(0)
      .setSize(1)
      .get()
    val mxhit: SearchHits = mxBiao.getHits
    if (mxhit.iterator().hasNext){
      val hit = mxhit.iterator().next()
      try{
        wxgdmx_spmc = hit.getSourceAsMap.get("spmc").toString
        wxgdmx_xlmc = hit.getSourceAsMap.get("xlmc").toString
        wxgdmx_gmsj = hit.getSourceAsMap.get("gmsj").toString
        wxgdmx_jxmc = hit.getSourceAsMap.get("jxmc").toString
        wxgdmx_gzxx = hit.getSourceAsMap.get("gzxx").toString
      }catch{
        case e:Exception => logger.error("TidbKuanBiaoFunction从明细表查询异常->" + e.getMessage)
      }
    }

    //初始化预约表相关字段
    var wxgdyy_kssj:String = "null"//开始预约时间
    var wxgdyy_jssj:String = "null"//结束预约时间
    var wxgdyy_yysj:String = "null"//预约时间
    var wxgdyy_czsj:String = "null"//预约操作时间
    var bi_yqwgsj:String = "null"//要求完工时间
    var bi_timeout:String = "0"//工单超时
    //根据主键查询预约表
    val YuYBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_APPOINTMENT_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .addSort("czsj",SortOrder.DESC)
      .setFrom(0)
      .setSize(1)
      .get()
    val YuYBiaoHits: SearchHits = YuYBiao.getHits

    if (YuYBiaoHits.iterator().hasNext) {
      val hit = YuYBiaoHits.iterator().next()
      try {
        wxgdyy_kssj = hit.getSourceAsMap.get("kssj").toString
        wxgdyy_jssj = hit.getSourceAsMap.get("jssj").toString
        wxgdyy_czsj = hit.getSourceAsMap.get("czsj").toString
        wxgdyy_yysj = wxgdyy_kssj +"-"+ wxgdyy_jssj
      } catch {
        case e: Exception => logger.error("TidbKuanBiaoFunction从预约表查询异常->" + e.getMessage)
      }
    }
    //完工时间为创建时间加一天
    if ("null".equals(wxgdyy_kssj)) {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      if("null".equals(wxgd_cjdt)){ //预约时间和创建时间都为空，福默认值1970-01-01
        val time1:String = "1997-01-01 00:00:00"
        val parse = simpleDateFormat.parse(time1)
        val calendar = Calendar.getInstance
        calendar.setTime(parse)
        bi_yqwgsj = simpleDateFormat.format(calendar.getTime())
      }else{ //预约时间为空，创建时间不为空
        val parse = simpleDateFormat.parse(wxgd_cjdt)
        val calendar = Calendar.getInstance
        calendar.setTime(parse)
        calendar.add(Calendar.DATE, 1)
        bi_yqwgsj = simpleDateFormat.format(calendar.getTime())
      }
      //完工时间为预约时间加一天
    } else {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val parse = simpleDateFormat.parse(wxgdyy_kssj)
      val calendar = Calendar.getInstance
      calendar.setTime(parse)
      calendar.add(Calendar.DATE, 1)
      bi_yqwgsj = simpleDateFormat.format(calendar.getTime())
    }

    //初始化完工反馈字段
    var wcsjParse: Date = simpleDateFormat.parse("1997-01-01 00:00:00") //订单已完成的完成时间
    var wxgdfkmx_wg_fknr:String = "null"// 完工反馈内容,
    var wxgdfkmx_wg_fksj:String = "null"//  维修点完工反馈时间
    //根据主键查反馈明细中完工内容
    val wg_fanKuiMxBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("pgid",value._1))
      .must(QueryBuilders.termQuery("fklb","完工")))
      .addSort("fksj",SortOrder.DESC)
      .setFrom(0)
      .setSize(1)
      .get()
    val wg_fanKuiMxBiaoHit: SearchHits = wg_fanKuiMxBiao.getHits

    if (wg_fanKuiMxBiaoHit.iterator().hasNext){
      try {
        val hit = wg_fanKuiMxBiaoHit.iterator().next()
        wxgdfkmx_wg_fknr = hit.getSourceAsMap.get("fknr").toString
        wxgdfkmx_wg_fksj = hit.getSourceAsMap.get("fksj").toString
      } catch {
        case e:Exception => logger.error("TidbKuanBiaoFunction查完工反馈明细表取最新一条异常->"+e.getMessage)
      }
    }

    if (!"null".equals(wxgdfkmx_wg_fksj)){
      wcsjParse = simpleDateFormat.parse(wxgdfkmx_wg_fksj)
    }
    //转换完成时间
    val wcsjCal = Calendar.getInstance
    wcsjCal.setTime(wcsjParse)
    //设置抓取当前时间，并转换成claendar对象计算时间
    val now =  new Date()
    val nowCal = Calendar.getInstance()
    nowCal.setTime(now)
    //期望完成时间格式转化
    val YQwangongtimeParse = simpleDateFormat.parse(util1.panduanDate(bi_yqwgsj))
    val YQwangongtimeCal = Calendar.getInstance
    YQwangongtimeCal.setTime(YQwangongtimeParse)

    //将状态字符串转为数字
//    val util: NumberFormatUtil = new NumberFormatUtil
//    var zhuangtai: Int = 0
//    try {
//      zhuangtai = util.panduanInt(wxgd_stat)
//    } catch {
//      case e: Exception => logger.error("状态转换异常->" + e.getMessage)
//    }
    //超时计算
//    if( zhuangtai > 40){ //计算完工超时时间
//        logger.info("完成时间为"+wcsjParse.toString)
//      if(wg_fanKuiMxBiaoHit.totalHits != 0 && wcsjCal.compareTo(YQwangongtimeCal) > 0){
//        bi_timeout =  ((wcsjCal.getTimeInMillis() - YQwangongtimeCal.getTimeInMillis())/3600d/1000d).formatted("%.1f").toString
//        logger.info("发送到完工超时，超时时长"+ bi_timeout)
//      }else{
//        bi_timeout = "0"
//      }
//    }else{//计算未完工超时时间
//       logger.info("要求完成时间："+YQwangongtimeParse.toString)
//       if ( YQwangongtimeCal.compareTo(nowCal) > 0){
//         bi_timeout =  ((YQwangongtimeCal.getTimeInMillis() - nowCal.getTimeInMillis())/3600d/1000d).formatted("%.1f").toString
//       }else{
//         bi_timeout = "0"
//       }
//    }


    //初始化驳回反馈信息字段
    var wxgdfkmx_bh_fklb:String = "null"// as 驳回类型,
    var wxgdfkmx_bh_fknr:String = "null"// as 驳回内容,
    var wxgdfkmx_bh_fksj:String = "null"// as 驳回时间,
    var wxgdfkmx_bh_fkwdmc:String = "null"// as 驳回操作网点,

    //根据主键查反馈明细中驳回
    val bh_fanKuiMxBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("pgid",value._1))
        .must(QueryBuilders.termQuery("fklb","完工")))
      .addSort("fksj",SortOrder.DESC)
      .setFrom(0)
      .setSize(1)
      .get()
    val bh_fanKuiMxBiaoHit: SearchHits = bh_fanKuiMxBiao.getHits

    if (bh_fanKuiMxBiaoHit.iterator().hasNext){
      try {
        val hit = bh_fanKuiMxBiaoHit.iterator().next()
        wxgdfkmx_bh_fklb = hit.getSourceAsMap.get("fklb").toString
        wxgdfkmx_bh_fknr = hit.getSourceAsMap.get("fknr").toString
        wxgdfkmx_bh_fksj = hit.getSourceAsMap.get("fksj").toString
        wxgdfkmx_bh_fkwdmc = hit.getSourceAsMap.get("fkwdmc").toString
      } catch {
        case e:Exception => logger.error("TidbKuanBiaoFunction查完工反馈明细表取最新一条异常->"+e.getMessage)
      }
    }


    //初始化用户待件字段
    var wxgdfkmx_dj:String = "null"//是否用户待件
    //根据pgid查询反馈明细表条件限定待件
    val dj_fkmxBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("fkjg","待件"))
      .should(QueryBuilders.termQuery("fkjg","待件变正常"))
      .should(QueryBuilders.termQuery("fkjg","待件已开单发货"))
      .should(QueryBuilders.termQuery("fkjg","待件已验收")))
      .get()
    val dj_fkmxhit: SearchHits = dj_fkmxBiao.getHits
      wxgdfkmx_dj = dj_fkmxhit.totalHits match {
        case 0 => "否"
        case _ => "是"
      }


    out.collect(KuanBiaoToTidb(wxgd_cjdt,wxgd_beiz,wxgd_zbby,wxgd_xxlb,wxgd_xxqd,wxgd_xxly,wxgd_wxwdno,wxgd_wxwdmc,wxgd_pgid,wxgd_yhsx,wxgd_quhao
      ,wxgd_xian,wxgd_stat,wxgd_cjren,wxgd_xjwdsj,wxgd_cjwdno,wxgd_xjwdno,wxgd_ssqy,wxgd_yhmc,wxgd_yddh,wxgd_sfen,wxgd_cshi,wxgd_spid,wxgd_spmc,wxgd_wxren
      ,wxgd_wxrenid,wxgd_yhqwsmsj,wxgd_qwsmjssj,wxgdyd_zxxs,wxgdmx_spmc,wxgdmx_xlmc,wxgdmx_gmsj,wxgdmx_jxmc,wxgdmx_gzxx,wxgdmyd_pjnr,wxgdyy_czsj,wxgdyy_kssj,wxgdyy_jssj
      ,wxgdyy_yysj,bi_yqwgsj,wxgdfkmx_wg_fknr,wxgdfkmx_wg_fksj,wxgdfkmx_bh_fklb,wxgdfkmx_bh_fknr,wxgdfkmx_bh_fksj,wxgdfkmx_bh_fkwdmc,wxgdfkmx_dj))

    ESTransportPoolUtil.returnClient(client)
  }
}
