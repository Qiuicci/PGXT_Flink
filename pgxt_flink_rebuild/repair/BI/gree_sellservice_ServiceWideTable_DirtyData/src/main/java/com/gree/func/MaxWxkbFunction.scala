package com.gree.func

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.gree.constant.Constant
import com.gree.model.MaxKuanBiao
import com.gree.util.{ESTransportPoolUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

class MaxWxkbFunction extends ProcessFunction[(String, Long), MaxKuanBiao]{
  override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), MaxKuanBiao]#Context, out: Collector[MaxKuanBiao]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(MaxWxkbFunction.super.getClass)
    //连接ES
    val ut = new NumberFormatUtil
    val client: TransportClient = ESTransportPoolUtil.getClient
    //宽表字段初始化赋值
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
    var fklb:String = "null"
    var fkjg:String = "null"
    var fksj:String = "null"
    var isOrNotDaiJian:String = "null"
    var baowangongtime:String = "null"
    var vip:String = "null"


    //根据主键pgid查询预约表所需指定字段
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
        appointmentkssj = hit.getSourceAsMap.get("kssj").toString
        appointmentjssj = hit.getSourceAsMap.get("jssj").toString
      } catch {
        case e: Exception => logger.error("MaxWxkbFunction从预约表查询异常->" + e.getMessage)
      }
    }

    //查询主表字段
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
        vip = hit.getSourceAsMap.get("vip").toString
      } catch {
        case e: Exception => logger.info("MaxWxkbFunction从主表查询异常->" + e.getMessage)
      }
    }

    //要求完工时间
    var yqwangongtime: String = "null"
    //如果预约时间为空，则要求完工时间为创建时间加一天
    if ("null".equals(appointmentkssj)) {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val parse = simpleDateFormat.parse(ut.panduanDate(cjdt))
      val calendar = Calendar.getInstance
      calendar.setTime(parse)
      calendar.add(Calendar.DATE, 1)
      yqwangongtime = simpleDateFormat.format(calendar.getTime())

      //完工时间为预约时间加一天
    } else {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val parse = simpleDateFormat.parse(appointmentkssj)
      val calendar = Calendar.getInstance
      calendar.setTime(parse)
      calendar.add(Calendar.DATE, 1)
      yqwangongtime = simpleDateFormat.format(calendar.getTime())
    }

    //查询反馈明细表并取最新一条
    val fanKuiMxBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .addSort("fksj",SortOrder.DESC)
      .setFrom(0)
      .setSize(1)
      .get()
    val fanKuiMxBiaoHit: SearchHits = fanKuiMxBiao.getHits

    if (fanKuiMxBiaoHit.iterator().hasNext){
      try {
        val hit = fanKuiMxBiaoHit.iterator().next()
        fklb = hit.getSourceAsMap.get("fklb").toString
        fkjg = hit.getSourceAsMap.get("fkjg").toString
        fksj = hit.getSourceAsMap.get("fksj").toString
      } catch {
        case e:Exception => logger.error("MaxWxkbFunction查反馈明细表取最新一条异常->"+e.getMessage)
      }
    }

    //报完工时间---查询反馈明细表，取fklb为完工，fksj最旧的一条
    var wgfksj:String = "null"
    val wgfankuimx:SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery.must(QueryBuilders.termQuery("pgid", value._1)).must(QueryBuilders.termQuery("fklb","完工")))
      .addSort("fksj",SortOrder.ASC)
      .setFrom(0)
      .setSize(1)
      .get()
    if (wgfankuimx.getHits.totalHits > 0){
      try{
        wgfksj = wgfankuimx.getHits.iterator().next().getSourceAsMap.get("fksj").toString
      }catch {
        case e:Exception => logger.error("MaxWxkbFunction查反馈明细表取完工最旧一条异常-> "+e.getMessage)
      }
    }
    //报完工时间--查询反馈明细表，取fklb为关闭状态工单的fksj 只有一条
    var gbfksj:String = "null"
    val gbfankuimx:SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery.must(QueryBuilders.termQuery("pgid", value._1)).must(QueryBuilders.termQuery("fklb","关闭")))
      .setFrom(0)
      .setSize(1)
      .get()
    if (gbfankuimx.getHits.totalHits >0){
      try{
        gbfksj = gbfankuimx.getHits.iterator().next().getSourceAsMap.get("fksj").toString
      }catch {
        case e:Exception => logger.error("MaxWxkbFunction查反馈明细表取关闭工单时间异常-> "+e.getMessage)
      }
    }
    if ("null".equals(wgfksj)){
      baowangongtime = gbfksj
    }else{
      baowangongtime = wgfksj
    }

    var level1: String = "null"
    var level2: String = "null"
    var level3: String = "null"
    var level4: String = "null"
    var level5: String = "null"
    var level6: String = "null"
    var level7: String = "null"

    if (!"null".equals(spid)) {
      //从quanxianlevel_wangdianlevel_t1 ES中查出对应字段
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
          case e: Exception => logger.error("MaxWxkbFunction查询quanxianlevel_wangdianlevel_v1 ES表异常"+e.getMessage)
        }
      }
    }
    //查询数据权限
    var level8:String = "null"
    var level9:String = "null"
    var level10:String = "null"
    var level11:String = "null"
    var level12:String = "null"
    var level13:String = "null"
    var level14:String = "null"
    if (!"null".equals(spid)) {
      //从quanxianlevel_wangdianlevel_v1 ES中查出对应字段
      val quanXianLevelSale: SearchResponse = client
        .prepareSearch(Constant.QUANXIANLEVEL_WANGDIANLEVEL_INDEX)
        .setTypes(Constant.ES_TYPE)
        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("stat", 2))
          .must(QueryBuilders.termQuery("fwlb", "销售"))
          .must(QueryBuilders.termQuery("wdno", cjwdno))
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
          case e: Exception => logger.error("MaxWxkbYuYueBiaoFunction查询quanxianlevel_wangdianlevel_v1销售 ES表异常" + e.getMessage)
        }
      }
    }
    //查询预约表
    val yuYueBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_APPOINTMENT_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val yuYueList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()


    val yuYueBiaoHits: SearchHits = yuYueBiao.getHits
    for (hit <- yuYueBiaoHits){
      try {
        val yuYueMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        yuYueMap.put("id",hit.getSourceAsMap.get("id").toString)
        yuYueMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        yuYueMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        yuYueMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        yuYueMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        yuYueMap.put("kssj",hit.getSourceAsMap.get("kssj").toString)
        yuYueMap.put("jssj",hit.getSourceAsMap.get("jssj").toString)
        yuYueMap.put("czren",hit.getSourceAsMap.get("czren").toString)
        yuYueMap.put("pgid",hit.getSourceAsMap.get("pgid").toString)
        yuYueMap.put("czsj",hit.getSourceAsMap.get("czsj").toString)
        yuYueMap.put("leix",hit.getSourceAsMap.get("leix").toString)
        yuYueMap.put("reason",hit.getSourceAsMap.get("reason").toString)
        yuYueMap.put("beiz",hit.getSourceAsMap.get("beiz").toString)
        yuYueList.add(yuYueMap)

      } catch {
        case e:Exception => logger.error("MaxWxkbFunction查询default_server_greeshservice_tbl_assign_appointment_t1异常"+e.getMessage)
      }
    }

    //查反馈明细表
    val fkmxBiao = client
      .prepareSearch(Constant.TBL_ASSIGN_FKMX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val fkmxList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()


    val fkmxBiaoHits: SearchHits = fkmxBiao.getHits
    for (hit <- fkmxBiaoHits){
      try {

        val fkmxMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        fkmxMap.put("fkid",hit.getSourceAsMap.get("fkid").toString)
        fkmxMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        fkmxMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        fkmxMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        fkmxMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        fkmxMap.put("pgid",hit.getSourceAsMap.get("pgid").toString)
        fkmxMap.put("fklb",hit.getSourceAsMap.get("fklb").toString)
        fkmxMap.put("fkjg",hit.getSourceAsMap.get("fkjg").toString)
        fkmxMap.put("fknr",hit.getSourceAsMap.get("fknr").toString.replace("\"", ""))
        fkmxMap.put("fkren",hit.getSourceAsMap.get("fkren").toString)
        fkmxMap.put("fkrenmc",hit.getSourceAsMap.get("fkrenmc").toString)
        fkmxMap.put("fksj",hit.getSourceAsMap.get("fksj").toString)
        fkmxMap.put("fkwdno",hit.getSourceAsMap.get("fkwdno").toString)
        fkmxMap.put("fkwdmc",hit.getSourceAsMap.get("fkwdmc").toString)
        fkmxMap.put("scid",hit.getSourceAsMap.get("scid").toString)
        fkmxMap.put("scwj",hit.getSourceAsMap.get("scwj").toString)
        fkmxMap.put("qqlyxh",hit.getSourceAsMap.get("qqlyxh").toString)
        fkmxMap.put("fkmxguid",hit.getSourceAsMap.get("fkmxguid").toString)
        fkmxMap.put("wjid",hit.getSourceAsMap.get("wjid").toString)
        fkmxList.add(fkmxMap)

      } catch {
        case e:Exception =>logger.error("MaxWxkbFunction查询default_server_greeshservice_tbl_assign_fkmx_t1异常"+e.getMessage)
      }
    }
    //查新信息表
    val xinXinXiBiao = client
      .prepareSearch(Constant.TBL_ASSIGN_XZYD_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val xinXiList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()


    val xinXinXiBiaoHits: SearchHits = xinXinXiBiao.getHits
    for (hit <- xinXinXiBiaoHits){
      try {

        val xinXiMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        xinXiMap.put("xzid",hit.getSourceAsMap.get("xzid").toString)
        xinXiMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        xinXiMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        xinXiMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        xinXiMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        xinXiMap.put("pgid",hit.getSourceAsMap.get("pgid").toString)
        xinXiMap.put("czren",hit.getSourceAsMap.get("czren").toString)
        xinXiMap.put("czsj",hit.getSourceAsMap.get("czsj").toString)
        xinXiMap.put("wdno",hit.getSourceAsMap.get("wdno").toString)
        xinXiMap.put("cshu",hit.getSourceAsMap.get("cshu").toString)
        xinXiMap.put("xzyq",hit.getSourceAsMap.get("xzyq").toString)
        xinXiMap.put("xzyqlb",hit.getSourceAsMap.get("xzyqlb").toString)
        xinXiMap.put("ydbz",hit.getSourceAsMap.get("ydbz").toString)
        xinXiMap.put("ydsj",hit.getSourceAsMap.get("ydsj").toString)
        xinXiMap.put("ydren",hit.getSourceAsMap.get("ydren").toString)
        xinXiMap.put("ydrmc",hit.getSourceAsMap.get("ydrmc").toString)
        xinXiMap.put("ydwd",hit.getSourceAsMap.get("ydwd").toString)
        xinXiMap.put("ydwdmc",hit.getSourceAsMap.get("ydwdmc").toString)
        xinXiList.add(xinXiMap)

      } catch {
        case e:Exception =>logger.error("MaxWxkbFunction查询default_server_greeshservice_tbl_assign_xzyd_t1异常"+e.getMessage)
      }
    }
    //查明细表
    val mingXiBiao = client
      .prepareSearch(Constant.TBL_ASSIGN_MX_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val mxList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()

    val mingXiBiaoHits: SearchHits = mingXiBiao.getHits
    for (hit <- mingXiBiaoHits){
      try {
        val mxMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        mxMap.put("pgmxid",hit.getSourceAsMap.get("pgmxid").toString)
        mxMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        mxMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        mxMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        mxMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        mxMap.put("pgid",hit.getSourceAsMap.get("pgid").toString)
        mxMap.put("spid",hit.getSourceAsMap.get("spid").toString)
        mxMap.put("spmc",hit.getSourceAsMap.get("spmc").toString)
        mxMap.put("xlid",hit.getSourceAsMap.get("xlid").toString)
        mxMap.put("xlmc",hit.getSourceAsMap.get("xlmc").toString)
        mxMap.put("xiid",hit.getSourceAsMap.get("xiid").toString)
        mxMap.put("ximc",hit.getSourceAsMap.get("ximc").toString)
        mxMap.put("jxid",hit.getSourceAsMap.get("jxid").toString)
        mxMap.put("jxmc",hit.getSourceAsMap.get("jxmc").toString)
        mxMap.put("jxno",hit.getSourceAsMap.get("jxno").toString)
        mxMap.put("gmsj",hit.getSourceAsMap.get("gmsj").toString)
        mxMap.put("xsdw",hit.getSourceAsMap.get("xsdw").toString)
        mxMap.put("xsdwdh",hit.getSourceAsMap.get("xsdwdh").toString)
        mxMap.put("fwdw",hit.getSourceAsMap.get("fwdw").toString)
        mxMap.put("fwdwdh",hit.getSourceAsMap.get("fwdwdh").toString)
        mxMap.put("gzwz",hit.getSourceAsMap.get("gzwz").toString)
        mxMap.put("gzxx",hit.getSourceAsMap.get("gzxx").toString)
        mxMap.put("czren",hit.getSourceAsMap.get("czren").toString)
        mxMap.put("czsj",hit.getSourceAsMap.get("czsj").toString)
        mxMap.put("czwd",hit.getSourceAsMap.get("czwd").toString)
        mxMap.put("njtm",hit.getSourceAsMap.get("njtm").toString)
        mxMap.put("wjtm",hit.getSourceAsMap.get("wjtm").toString)
        mxMap.put("beiz",hit.getSourceAsMap.get("beiz").toString)
        mxMap.put("njtm2",hit.getSourceAsMap.get("njtm2").toString)
        mxMap.put("qqlyxh",hit.getSourceAsMap.get("qqlyxh").toString)
        mxMap.put("pinpai",hit.getSourceAsMap.get("pinpai").toString)
        mxMap.put("fee",hit.getSourceAsMap.get("fee").toString)
        mxMap.put("xxfee",hit.getSourceAsMap.get("xxfee").toString)
        mxMap.put("hsqk",hit.getSourceAsMap.get("hsqk").toString)
        mxMap.put("gzxxid",hit.getSourceAsMap.get("gzxxid").toString)
        mxMap.put("shul",hit.getSourceAsMap.get("shul").toString)
        mxMap.put("wwsl",hit.getSourceAsMap.get("wwsl").toString)
        mxMap.put("wxcount",hit.getSourceAsMap.get("wxcount").toString)
        mxMap.put("tmjscount",hit.getSourceAsMap.get("tmjscount").toString)
        mxMap.put("yblength",hit.getSourceAsMap.get("yblength").toString)
        mxMap.put("bxdue",hit.getSourceAsMap.get("bxdue").toString)
        mxList.add(mxMap)

      } catch {
        case e:Exception => logger.error("MaxWxkbFunction查询default_server_greeshservice_tbl_assign_mx_t1异常"+e.getMessage)
      }
    }
    //查满意表
    val manYiBiao = client
      .prepareSearch(Constant.TBL_ASSIGN_SATISFACTION_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val manYiList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()


    val manYiBiaoHits: SearchHits = manYiBiao.getHits
    for (hit <- manYiBiaoHits){
      try {
        val manYiMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        manYiMap.put("id",hit.getSourceAsMap.get("id").toString)
        manYiMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        manYiMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        manYiMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        manYiMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        manYiMap.put("pgid",hit.getSourceAsMap.get("pgid").toString)
        manYiMap.put("pjly",hit.getSourceAsMap.get("pjly").toString)
        manYiMap.put("pjnr",hit.getSourceAsMap.get("pjnr").toString)
        manYiMap.put("hfren",hit.getSourceAsMap.get("hfren").toString)
        manYiMap.put("hfwdmc",hit.getSourceAsMap.get("hfwdmc").toString)
        manYiMap.put("hfwdno",hit.getSourceAsMap.get("hfwdno").toString)
        manYiMap.put("hfsj",hit.getSourceAsMap.get("hfsj").toString)
        manYiMap.put("bmylx",hit.getSourceAsMap.get("bmylx").toString)
        manYiMap.put("bmybeiz",hit.getSourceAsMap.get("bmybeiz").toString)
        manYiMap.put("bmysj",hit.getSourceAsMap.get("bmysj").toString)
        manYiMap.put("splb",hit.getSourceAsMap.get("splb").toString)
        manYiMap.put("mydlx",hit.getSourceAsMap.get("mydlx").toString)
        manYiMap.put("sxlx",hit.getSourceAsMap.get("sxlx").toString)
        manYiList.add(manYiMap)

      } catch {
        case e:Exception => logger.error("MaxWxkbFunction查询default_server_greeshservice_tbl_assign_satisfaction_t1异常"+e.getMessage)
      }
    }
    //查反馈表
    val fanKuiBiao = client
      .prepareSearch(Constant.TBL_ASSIGN_FEEDBACK_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val fanKuiList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()

    val fanKuiBiaoHits = fanKuiBiao.getHits
    for (hit <- fanKuiBiaoHits){
      try {
        val fanKuiMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        fanKuiMap.put("id",hit.getSourceAsMap.get("id").toString)
        fanKuiMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        fanKuiMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        fanKuiMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        fanKuiMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        fanKuiMap.put("pgid",hit.getSourceAsMap.get("pgid").toString)
        fanKuiMap.put("zlfksj",hit.getSourceAsMap.get("zlfksj").toString)
        fanKuiMap.put("zlfkbh",hit.getSourceAsMap.get("zlfkbh").toString)
        fanKuiMap.put("czren",hit.getSourceAsMap.get("czren").toString)
        fanKuiMap.put("czsj",hit.getSourceAsMap.get("czsj").toString)
        fanKuiList.add(fanKuiMap)
      } catch {
        case e:Exception =>logger.error("MaxWxkbFunction查询default_server_greeshservice_tbl_assign_feedback_t1异常"+e.getMessage)
      }
    }
    //查待件表
    val daiJianBiao = client
      .prepareSearch(Constant.TBL_ASSIGN_DAIJIAN_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.termQuery("pgid", value._1))
      .get()

    val daiJianList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()
    val daiJianBiaoHits: SearchHits = daiJianBiao.getHits
    //根据反查数据库状态判断是否待件
    isOrNotDaiJian = daiJianBiaoHits.totalHits match {
      case 0  =>  "否"
      case _   => "是"
    }
    for (hit <- daiJianBiaoHits){
      try {
        val daiJianMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        daiJianMap.put("id",hit.getSourceAsMap.get("id").toString)
        daiJianMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        daiJianMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        daiJianMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        daiJianMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        daiJianMap.put("pjsqbh",hit.getSourceAsMap.get("pjsqbh").toString)
        daiJianMap.put("pjsqsj",hit.getSourceAsMap.get("pjsqsj").toString)
        daiJianMap.put("pjwlbm",hit.getSourceAsMap.get("pjwlbm").toString)
        daiJianMap.put("pjwlmc",hit.getSourceAsMap.get("pjwlmc").toString)
        daiJianMap.put("pjwlsl",hit.getSourceAsMap.get("pjwlsl").toString)
        daiJianMap.put("djquyunum",hit.getSourceAsMap.get("djquyunum").toString)
        daiJianMap.put("djxsgsnum",hit.getSourceAsMap.get("djxsgsnum").toString)
        daiJianMap.put("djwdnum",hit.getSourceAsMap.get("djwdnum").toString)
        daiJianMap.put("pgid",hit.getSourceAsMap.get("pgid").toString)
        daiJianMap.put("djwd",hit.getSourceAsMap.get("djwd").toString)
        daiJianMap.put("djwdmc",hit.getSourceAsMap.get("djwdmc").toString)
        daiJianMap.put("djsj",hit.getSourceAsMap.get("djsj").toString)
        daiJianMap.put("splb",hit.getSourceAsMap.get("splb").toString)
        daiJianMap.put("cjdt",hit.getSourceAsMap.get("cjdt").toString)
        daiJianMap.put("thwlbm",hit.getSourceAsMap.get("thwlbm").toString)
        daiJianMap.put("thwlmc",hit.getSourceAsMap.get("thwlmc").toString)
        daiJianMap.put("thbmxsgsnum",hit.getSourceAsMap.get("thbmxsgsnum").toString)
        daiJianMap.put("thbmqynum",hit.getSourceAsMap.get("thbmqynum").toString)
        daiJianMap.put("thbmwdnum",hit.getSourceAsMap.get("thbmwdnum").toString)
        daiJianMap.put("pjxtflag",hit.getSourceAsMap.get("pjxtflag").toString)
        daiJianList.add(daiJianMap)

      } catch {
        case e:Exception =>logger.error("MaxWxkbFunction查询default_server_greeshservice_tbl_assign_daijian_t1异常"+e.getMessage)
      }
    }
    //查询ES 的信息Tbl_Assign_Xzyd,记录未阅读信息数
    val xinXinXiResponse: SearchResponse = client
      .prepareSearch(Constant.TBL_ASSIGN_XZYD_INDEX)
      .setTypes(Constant.ES_TYPE)
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("pgid", value._1))
        .must(QueryBuilders.termQuery("ydbz",0)))
      .get()

    val unReadXxx: String = xinXinXiResponse.getHits.totalHits.toString
    try {
      out.collect(MaxKuanBiao(
        pgid, created_by, created_date, last_modified_by, last_modified_date, AUTH_STATE, azsl,
        beiz, bjustat, chaoshiqe, cjdt, cjren, cjrmc, cjwdno, cshi, cshiid, cxyzm,
        dhhm, dizi, dqjdsj, email, fjhm, fwrybwgsj, gdhao, gpsdzxx, jindu, ldcs, pgguid, qqlymc, qqlyxh, qqlyzj,
        quhao, qwsmjssj, qystat, sfen, sfenid, sffswx, spid, spmc, ssqy, stat, tsdengji, wcsj, weidu, wwsl, wxren, wxrenid,
        wxshul, wxwdmc, wxwdno, xian, xianid, xjwdmc, xjwdno, xjwdsj, xqxiaolei, xsdh, xsorsh, xswdmc, xswdno, xxlb, xxlbid, xxly,
        xxlyid, xxqd, xxqdid, xzhen, yddh, yddh2, yhgyhf, yhif, yhmc, yhqwsmsj, yhsx, yhyyczsj, yxji, zbby, zjczsj, zjczwd, zjczwdxtbh,
        zptype, zxhao, yhsxid, xzhenid, yuYueList,fkmxList,xinXiList,daiJianList,fanKuiList,mxList, manYiList, level1, level2, level3,
        level4, level5, level6, level7,level8,level9,level10,level11,level12,level13,level14,appointmentkssj,appointmentjssj,yqwangongtime,
        fklb,fkjg,fksj,unReadXxx,wxcount,extjson1,extjson2,extjson3,extjson4,extjson5,isOrNotDaiJian,vip,baowangongtime))
    } catch {
      case e:Exception => logger.error("MaxWxkbFunction输出出错->"+e.getCause+"异常->"+e.getMessage)
    }
    ESTransportPoolUtil.returnClient(client)
  }
}
