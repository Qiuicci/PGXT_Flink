package com.gree.func

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.gree.constant.Constant
import com.gree.model.WxDataChaoShi
import com.gree.util.{ESTransportPoolUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.slf4j.{Logger, LoggerFactory}

class ChaoShiKanBanFunction(chaoshi:OutputTag[WxDataChaoShi], weiwangongchaoshi:OutputTag[WxDataChaoShi], wangongweichaoshi:OutputTag[WxDataChaoShi])  extends  ProcessFunction[(String,Long),WxDataChaoShi]{
  override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), WxDataChaoShi]#Context, out: Collector[WxDataChaoShi]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(ChaoShiKanBanFunction.super.getClass)
    //es链接配置
    val client: TransportClient = ESTransportPoolUtil.getClient
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var timeout:String = "null"
    val util1 = new NumberFormatUtil

    var pgid: String = "null"
    var created_by: String = "null"
    var created_date: String = "null"
    var last_modified_by: String = "null"
    var last_modified_date: String = "null"
    var AUTH_STATE: String = "null"
    var azsl: String = "null"
    var beiz: String = "null"
    var bjustat: String = "null"
    var chaoshiqe: String = "null"
    var cjdt: String = "null"
    var cjren: String = "null"
    var cjrmc: String = "null"
    var cjwdno: String = "null"
    var cshi: String = "null"
    var cxyzm: String = "null"
    var dhhm: String = "null"
    var dizi: String = "null"
    var dqjdsj: String = "null"
    var email: String = "null"
    var fjhm: String = "null"
    var fwrybwgsj: String = "null"
    var gdhao: String = "null"
    var gpsdzxx: String = "null"
    var jindu: String = "null"
    var ldcs: String = "null"
    var pgguid: String = "null"
    var qqlymc: String = "null"
    var qqlyxh: String = "null"
    var qqlyzj: String = "null"
    var quhao: String = "null"
    var qwsmjssj: String = "null"
    var qystat: String = "null"
    var sfen: String = "null"
    var sffswx: String = "null"
    var spid: String = "null"
    var spmc: String = "null"
    var ssqy: String = "null"
    var stat: String = "null"
    var tsdengji: String = "null"
    var wcsj: String = "null"
    var weidu: String = "null"
    var wwsl: String = "null"
    var wxren: String = "null"
    var wxrenid: String = "null"
    var wxshul: String = "null"
    var wxwdmc: String = "null"
    var wxwdno: String = "null"
    var xian: String = "null"
    var xjwdmc: String = "null"
    var xjwdno: String = "null"
    var xjwdsj: String = "null"
    var xqxiaolei: String = "null"
    var xsdh: String = "null"
    var xsorsh: String = "null"
    var xswdmc: String = "null"
    var xswdno: String = "null"
    var xxlb: String = "null"
    var xxly: String = "null"
    var xxqd: String = "null"
    var xzhen: String = "null"
    var yddh: String = "null"
    var yddh2: String = "null"
    var yhgyhf: String = "null"
    var yhif: String = "null"
    var yhmc: String = "null"
    var yhqwsmsj: String = "null"
    var yhsx: String = "null"
    var yhyyczsj: String = "null"
    var yxji: String = "null"
    var zbby: String = "null"
    var zjczsj: String = "null"
    var zjczwd: String = "null"
    var zjczwdxtbh: String = "null"
    var zptype: String = "null"
    var zxhao: String = "null"
    var cshiid: String = "null"
    var sfenid: String = "null"
    var xianid: String = "null"
    var xxlbid: String = "null"
    var xxlyid: String = "null"
    var xxqdid: String = "null"
    var yhsxid: String = "null"
    var xzhenid: String = "null"
    var wxcount:String = "null"
    var extjson1:String = "null"
    var extjson2:String = "null"
    var extjson3:String = "null"
    var extjson4:String = "null"
    var extjson5:String = "null"
    var appointmentkssj: String = "null"
    var appointmentjssj: String = "null"

    //写查询语句查出主表需要的字段
    val response: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val hits: SearchHits = response.getHits
    if (hits.iterator().hasNext) {
      val hit = hits.iterator().next()
      logger.info("对应结果为->" + hit.getSourceAsString)
      logger.info("对应状态结果->" + hit.getSourceAsMap.get("stat"))
      try {
        pgid = hit.getSourceAsMap.get("pgid").toString
        created_by = hit.getSourceAsMap.get("created_by").toString
        created_date = hit.getSourceAsMap.get("created_date").toString
        last_modified_by = hit.getSourceAsMap.get("last_modified_by").toString
        last_modified_date = hit.getSourceAsMap.get("last_modified_date").toString
        AUTH_STATE = hit.getSourceAsMap.get("auth_state").toString
        azsl = hit.getSourceAsMap.get("azsl").toString
        beiz = hit.getSourceAsMap.get("beiz").toString
        bjustat = hit.getSourceAsMap.get("bjustat").toString
        chaoshiqe = hit.getSourceAsMap.get("chaoshiqe").toString
        cjdt = hit.getSourceAsMap.get("cjdt").toString
        cjren = hit.getSourceAsMap.get("cjren").toString
        cjrmc = hit.getSourceAsMap.get("cjrmc").toString
        cjwdno = hit.getSourceAsMap.get("cjwdno").toString
        cshi = hit.getSourceAsMap.get("cshi").toString
        cxyzm = hit.getSourceAsMap.get("cxyzm").toString
        dhhm = hit.getSourceAsMap.get("dhhm").toString
        dizi = hit.getSourceAsMap.get("dizi").toString
        dqjdsj = hit.getSourceAsMap.get("dqjdsj").toString
        email = hit.getSourceAsMap.get("email").toString
        fjhm = hit.getSourceAsMap.get("fjhm").toString
        fwrybwgsj = hit.getSourceAsMap.get("fwrybwgsj").toString
        gdhao = hit.getSourceAsMap.get("gdhao").toString
        gpsdzxx = hit.getSourceAsMap.get("gpsdzxx").toString
        jindu = hit.getSourceAsMap.get("jindu").toString
        ldcs = hit.getSourceAsMap.get("ldcs").toString
        pgguid = hit.getSourceAsMap.get("pgguid").toString
        qqlymc = hit.getSourceAsMap.get("qqlymc").toString
        qqlyxh = hit.getSourceAsMap.get("qqlyxh").toString
        qqlyzj = hit.getSourceAsMap.get("qqlyzj").toString
        quhao = hit.getSourceAsMap.get("quhao").toString
        qwsmjssj = hit.getSourceAsMap.get("qwsmjssj").toString
        qystat = hit.getSourceAsMap.get("qystat").toString
        sfen = hit.getSourceAsMap.get("sfen").toString
        sffswx = hit.getSourceAsMap.get("sffswx").toString
        spid = hit.getSourceAsMap.get("spid").toString
        spmc = hit.getSourceAsMap.get("spmc").toString
        ssqy = hit.getSourceAsMap.get("ssqy").toString
        stat = hit.getSourceAsMap.get("stat").toString
        tsdengji = hit.getSourceAsMap.get("tsdengji").toString
        wcsj = hit.getSourceAsMap.get("wcsj").toString
        weidu = hit.getSourceAsMap.get("weidu").toString
        wwsl = hit.getSourceAsMap.get("stat").toString
        wxren = hit.getSourceAsMap.get("wxren").toString
        wxrenid = hit.getSourceAsMap.get("wxrenid").toString
        wxshul = hit.getSourceAsMap.get("wxshul").toString
        wxwdmc = hit.getSourceAsMap.get("wxwdmc").toString
        wxwdno = hit.getSourceAsMap.get("wxwdno").toString
        xian = hit.getSourceAsMap.get("xian").toString
        xjwdmc = hit.getSourceAsMap.get("xjwdmc").toString
        xjwdno = hit.getSourceAsMap.get("xjwdno").toString
        xjwdsj = hit.getSourceAsMap.get("xjwdsj").toString
        xqxiaolei = hit.getSourceAsMap.get("xqxiaolei").toString
        xsdh = hit.getSourceAsMap.get("xsdh").toString
        xsorsh = hit.getSourceAsMap.get("xsorsh").toString
        xswdmc = hit.getSourceAsMap.get("xswdmc").toString
        xswdno = hit.getSourceAsMap.get("xswdno").toString
        xxlb = hit.getSourceAsMap.get("xxlb").toString
        xxly = hit.getSourceAsMap.get("xxly").toString
        xxqd = hit.getSourceAsMap.get("xxqd").toString
        xzhen = hit.getSourceAsMap.get("xzhen").toString
        yddh = hit.getSourceAsMap.get("yddh").toString
        yddh2 = hit.getSourceAsMap.get("yddh2").toString
        yhgyhf = hit.getSourceAsMap.get("yhgyhf").toString
        yhif = hit.getSourceAsMap.get("yhif").toString
        yhmc = hit.getSourceAsMap.get("yhmc").toString
        yhqwsmsj = hit.getSourceAsMap.get("yhqwsmsj").toString
        yhsx = hit.getSourceAsMap.get("yhsx").toString
        yhyyczsj = hit.getSourceAsMap.get("yhyyczsj").toString
        yxji = hit.getSourceAsMap.get("yxji").toString
        zbby = hit.getSourceAsMap.get("zbby").toString
        zjczsj = hit.getSourceAsMap.get("zjczsj").toString
        zjczwd = hit.getSourceAsMap.get("zjczwd").toString
        zjczwdxtbh = hit.getSourceAsMap.get("zjczwdxtbh").toString
        zptype = hit.getSourceAsMap.get("zptype").toString
        zxhao = hit.getSourceAsMap.get("zxhao").toString
        cshiid = hit.getSourceAsMap.get("cshiid").toString
        sfenid = hit.getSourceAsMap.get("sfenid").toString
        xianid = hit.getSourceAsMap.get("xianid").toString
        xxlbid = hit.getSourceAsMap.get("xxlbid").toString
        xxlyid = hit.getSourceAsMap.get("xxlyid").toString
        xxqdid = hit.getSourceAsMap.get("xxqdid").toString
        yhsxid = hit.getSourceAsMap.get("yhsxid").toString
        xzhenid = hit.getSourceAsMap.get("xzhenid").toString
        wxcount = hit.getSourceAsMap.get("wxcount").toString
        extjson1 = hit.getSourceAsMap.get("extjson1").toString
        extjson2 = hit.getSourceAsMap.get("extjson2").toString
        extjson3 = hit.getSourceAsMap.get("extjson3").toString
        extjson4 = hit.getSourceAsMap.get("extjson4").toString
        extjson5 = hit.getSourceAsMap.get("extjson5").toString
      } catch {
        case e: Exception => logger.error("从主表查询异常->" + e.getMessage)
      }
    }
    var level1:String = "null"
    var level2:String = "null"
    var level3:String = "null"
    var level4:String = "null"
    var level5:String = "null"
    var level6:String = "null"
    var level7:String = "null"
    if (!"null".equals(spid)) {
      //从quanxianlevel_wangdianlevel_v1 ES中查出售后权限对应字段
      val quanXianLevel: SearchResponse = client
        .prepareSearch(Constant.QUANXIANLEVEL_WANGDIANLEVEL_INDEX)
        .setTypes(Constant.ES_TYPE)
        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("stat", 2)).must(QueryBuilders.termQuery("fwlb", "售后"))
          .must(QueryBuilders.termQuery("wdno", xjwdno)).must(QueryBuilders.termQuery("splb", Integer.valueOf(spid))))
        .get()

      if (quanXianLevel.getHits.iterator().hasNext) {
        val hit: SearchHit = quanXianLevel.getHits.iterator().next()
        try {
          level1 = hit.getSourceAsMap.get("first").toString
          level2 = hit.getSourceAsMap.get("second").toString
          level3 = hit.getSourceAsMap.get("third").toString
          level4 = hit.getSourceAsMap.get("fourth").toString
          level5 = hit.getSourceAsMap.get("fifth").toString
          level6 = hit.getSourceAsMap.get("sixth").toString
          level7 = hit.getSourceAsMap.get("seventh").toString
        } catch {
          case e: Exception => logger.error("ChaoShiKanBanFunction查询quanxianlevel_wangdianlevel_v1 ES表异常" + e.getMessage)
        }
      }
    }

    var level8:String = "null"
    var level9:String = "null"
    var level10:String = "null"
    var level11:String = "null"
    var level12:String = "null"
    var level13:String = "null"
    var level14:String = "null"
    //查询数据权限
    if (!"null".equals(spid)) {
      //从quanxianlevel_wangdianlevel_t1 ES中查出销售权限对应字段
      val quanXianLevelSale: SearchResponse = client
        .prepareSearch(Constant.QUANXIANLEVEL_WANGDIANLEVEL_INDEX)
        .setTypes(Constant.ES_TYPE)
        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("stat", 2))
          .must(QueryBuilders.termQuery("fwlb", "销售"))
          .must(QueryBuilders.termQuery("wdno", xjwdno))
          .must(QueryBuilders.termQuery("splb", Integer.valueOf(spid))))
        .get()

      if (quanXianLevelSale.getHits.iterator().hasNext) {
        val hit: SearchHit = quanXianLevelSale.getHits.iterator().next()
        try {
          level8 = hit.getSourceAsMap.get("first").toString
          level9 = hit.getSourceAsMap.get("second").toString
          level10 = hit.getSourceAsMap.get("third").toString
          level11 = hit.getSourceAsMap.get("fourth").toString
          level12 = hit.getSourceAsMap.get("fifth").toString
          level13 = hit.getSourceAsMap.get("sixth").toString
          level14 = hit.getSourceAsMap.get("seventh").toString
        } catch {
          case e: Exception => logger.error("ChaoShiKanBanFunction2查询quanxianlevel_wangdianlevel_v1销售 ES表异常" + e.getMessage)
        }
      }
    }
    //将状态字符串转为数字
    val util: NumberFormatUtil = new NumberFormatUtil
    var zhuangtai: Int = 0
    try {
      zhuangtai = util.panduanInt(stat)
    } catch {
      case e: Exception => logger.error("状态转换异常->" + e.getMessage)
    }

    //写查询语句查出预约表需要的字段
    val YuYBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_APPOINTMENT_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .addSort("czsj", SortOrder.DESC)
      .setFrom(0)
      .setSize(1)
      .get()
    val YuYBiaoHits: SearchHits = YuYBiao.getHits

    if (YuYBiaoHits.iterator().hasNext) {
      val hit = YuYBiaoHits.iterator().next()
      logger.info("对应结果为->" + hit.getSourceAsString)
      try {
        appointmentkssj = hit.getSourceAsMap.get("kssj").toString
        appointmentjssj = hit.getSourceAsMap.get("jssj").toString
      } catch {
        case e: Exception => logger.error("从预约表查询异常->" + e.getMessage)
      }
    }
    //要求完工时间
    var YQwangongtime: String = "null"
    //如果预约时间为空，则要求完工时间为创建时间加一天
    try {
      if ("null".equals(appointmentkssj)) {
        if (hits.totalHits!=0){
          val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val parse = simpleDateFormat.parse(util1.panduanDate(cjdt))
          val calendar = Calendar.getInstance
          calendar.setTime(parse)
          calendar.add(Calendar.DATE, 1)
          YQwangongtime = simpleDateFormat.format(calendar.getTime())
        }
        //完工时间为预约时间加一天
      } else {
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val parse = simpleDateFormat.parse(appointmentkssj)
        val calendar = Calendar.getInstance
        calendar.setTime(parse)
        calendar.add(Calendar.DATE, 1)
        YQwangongtime = simpleDateFormat.format(calendar.getTime())
      }
    } catch {
      case e:Exception =>logger.error("ChaoShiKanBanFunction时间转换异常->"+e.getMessage)
    }

    //从反馈明细表查出报完工时间
    val fkmxBiaoWanGong = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("pgid", value._1)).must(QueryBuilders.termQuery("fklb", "完工")))
      .addSort("fksj", SortOrder.ASC)
      .setFrom(0)
      .setSize(1)
      .get()

    var baowangongtime: String = "null"
    logger.info("从反馈明细表查出报完工时间总数为" + fkmxBiaoWanGong.getHits.totalHits)
    //从反馈明细表查出报完工时间
    val fkmxBiaoWanGongHits = fkmxBiaoWanGong.getHits
    if (fkmxBiaoWanGongHits.iterator().hasNext) {
      val hit = fkmxBiaoWanGongHits.iterator().next()
      logger.info("从反馈明细表查出报完工时间->" + hit.getSourceAsString)
      baowangongtime = hit.getSourceAsMap.get("fksj").toString
    }

    var wcsjParse: Date = simpleDateFormat.parse("1997-01-01 00:00:00") //订单已完成的完成时间

    try {
      if (!"null".equals(baowangongtime)){
        wcsjParse = simpleDateFormat.parse(baowangongtime)  //订单已完成的完成时间
      }else{
        //从反馈明细表查出报完工时间
        val fkmxBiaoWanGong = client
          .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
          .setTypes(Constant.ES_TYPE)
          .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("pgid", value._1)).must(QueryBuilders.termQuery("fklb", "关闭")))
          .get()

        //从反馈明细表查出关闭工时间
        val fkmxBiaoWanGongHits = fkmxBiaoWanGong.getHits
        if (fkmxBiaoWanGongHits.iterator().hasNext){
          val hit = fkmxBiaoWanGongHits.iterator().next()
          logger.info("从反馈明细表查出关闭时间->"+hit.getSourceAsString)
          baowangongtime = hit.getSourceAsMap.get("fksj").toString
          wcsjParse = simpleDateFormat.parse(baowangongtime)
        }

      }

    } catch {
      case e: Exception => logger.error("ChaoShiKanBanFunction1反馈完工时间转换异常" + e.getMessage)
    }

    //转换完成时间
    val wcsjCal = Calendar.getInstance
    wcsjCal.setTime(wcsjParse)

    //期望完成时间格式转化
    val YQwangongtimeParse = simpleDateFormat.parse(util1.panduanDate(YQwangongtime))
    val YQwangongtimeCal = Calendar.getInstance
    YQwangongtimeCal.setTime(YQwangongtimeParse)

    //完工工单的预约时间+24小时 与当前时间
    if(zhuangtai > 40) {


      //从未完工mysql中删除已完工数据

      logger.info("从未完成工单删除完工工单")
      logger.info("完成时间为"+wcsjParse.toString)

      ctx.output[WxDataChaoShi](wangongweichaoshi,WxDataChaoShi(
        value._1, created_by, created_date, last_modified_by, last_modified_date, AUTH_STATE, azsl,
        beiz,bjustat,chaoshiqe,cjdt,cjren,cjrmc,cjwdno,cshi,cxyzm,
        dhhm,dizi,dqjdsj,email,fjhm,fwrybwgsj,gdhao,gpsdzxx,jindu,ldcs,pgguid,qqlymc,qqlyxh,qqlyzj,
        quhao,qwsmjssj,qystat,sfen,sffswx,spid,spmc,ssqy,stat,tsdengji,wcsj,weidu,wwsl,wxren,wxrenid,
        wxshul,wxwdmc,wxwdno,xian,xjwdmc,xjwdno,xjwdsj,xqxiaolei,xsdh,xsorsh,xswdmc,xswdno,xxlb,xxly,
        xxqd,xzhen,yddh,yddh2,yhgyhf,yhif,yhmc,yhqwsmsj,yhsx,yhyyczsj,yxji,zbby,zjczsj,zjczwd,zjczwdxtbh,
        zptype,zxhao,cshiid,sfenid,xianid,xxlbid,xxlyid,xxqdid,yhsxid,xzhenid,wxcount,extjson1,extjson2,extjson3,extjson4,extjson5,appointmentkssj,appointmentjssj,YQwangongtime,timeout,
        level1,level2,level3,level4,level5,level6, level7,level8,level9,level10,level11,level12,level13,level14))


      if(fkmxBiaoWanGongHits.totalHits != 0 && wcsjCal.compareTo(YQwangongtimeCal) > 0) {
        //将带派工数据发出
        timeout =  ((wcsjCal.getTimeInMillis() - YQwangongtimeCal.getTimeInMillis())/3600d/1000d).formatted("%.1f").toString

        logger.info("发送到完工超时，超时时长"+ timeout)

        ctx.output[WxDataChaoShi](chaoshi,WxDataChaoShi(
          value._1, created_by, created_date, last_modified_by, last_modified_date, AUTH_STATE, azsl,
          beiz,bjustat,chaoshiqe,cjdt,cjren,cjrmc,cjwdno,cshi,cxyzm,
          dhhm,dizi,dqjdsj,email,fjhm,fwrybwgsj,gdhao,gpsdzxx,jindu,ldcs,pgguid,qqlymc,qqlyxh,qqlyzj,
          quhao,qwsmjssj,qystat,sfen,sffswx,spid,spmc,ssqy,stat,tsdengji,wcsj,weidu,wwsl,wxren,wxrenid,
          wxshul,wxwdmc,wxwdno,xian,xjwdmc,xjwdno,xjwdsj,xqxiaolei,xsdh,xsorsh,xswdmc,xswdno,xxlb,xxly,
          xxqd,xzhen,yddh,yddh2,yhgyhf,yhif,yhmc,yhqwsmsj,yhsx,yhyyczsj,yxji,zbby,zjczsj,zjczwd,zjczwdxtbh,
          zptype,zxhao,cshiid,sfenid,xianid,xxlbid,xxlyid,xxqdid,yhsxid,xzhenid,wxcount,extjson1,extjson2,extjson3,extjson4,extjson5,appointmentkssj,appointmentjssj,YQwangongtime,timeout,
          level1,level2,level3,level4,level5,level6, level7,level8,level9,level10,level11,level12,level13,level14))
      }else{
        try{
          client.prepareDelete(Constant.WXKB_TIMEOOUT_ORDERS, Constant.ES_TYPE, value._1).get()
          logger.info("从完工超时中删除非超时数据")
        }catch {
          case e:Exception => logger.error("从完工超时中删除非超时数据异常："+e.getMessage)
        }
      }
    }else{

      try{
        client.prepareDelete(Constant.WXKB_TIMEOOUT_ORDERS, Constant.ES_TYPE, value._1).get()
        logger.info("从完工超时中删除 未完工的数据")
      }catch {
        case e:Exception => logger.error("从完工超时中删除未完工的数据异常："+e.getMessage)
      }


      //未完工工单进入mysql

      logger.info("发送到mysql未完成工单")

      ctx.output[WxDataChaoShi](weiwangongchaoshi,WxDataChaoShi(
        value._1, created_by, created_date, last_modified_by, last_modified_date, AUTH_STATE, azsl,
        beiz,bjustat,chaoshiqe,cjdt,cjren,cjrmc,cjwdno,cshi,cxyzm,
        dhhm,dizi,dqjdsj,email,fjhm,fwrybwgsj,gdhao,gpsdzxx,jindu,ldcs,pgguid,qqlymc,qqlyxh,qqlyzj,
        quhao,qwsmjssj,qystat,sfen,sffswx,spid,spmc,ssqy,stat,tsdengji,wcsj,weidu,wwsl,wxren,wxrenid,
        wxshul,wxwdmc,wxwdno,xian,xjwdmc,xjwdno,xjwdsj,xqxiaolei,xsdh,xsorsh,xswdmc,xswdno,xxlb,xxly,
        xxqd,xzhen,yddh,yddh2,yhgyhf,yhif,yhmc,yhqwsmsj,yhsx,yhyyczsj,yxji,zbby,zjczsj,zjczwd,zjczwdxtbh,
        zptype,zxhao,cshiid,sfenid,xianid,xxlbid,xxlyid,xxqdid,yhsxid,xzhenid,wxcount,extjson1,extjson2,extjson3,extjson4,extjson5,appointmentkssj,appointmentjssj,YQwangongtime,timeout,
        level1,level2,level3,level4,level5,level6, level7,level8,level9,level10,level11,level12,level13,level14))
    }
    ESTransportPoolUtil.returnClient(client)
  }
}
