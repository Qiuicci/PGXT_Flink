package com.gree.func

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.gree.constant.Constant
import com.gree.model.AZMaxOrder
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

class Max_Az_Order_Func extends ProcessFunction[(String,String,Long),AZMaxOrder]{
  val logger: Logger = LoggerFactory.getLogger(Max_Az_Order_Func.super.getClass)

  /**
    *
    * @param value(pgguid,table,ts)
    * @param ctx(pgguid,table,ts)
    * @param out(pgguid,table,ts)
    */
  override def processElement(value: (String, String, Long), ctx: ProcessFunction[(String, String, Long), AZMaxOrder]#Context, out: Collector[AZMaxOrder]): Unit = {
    //获取PGGUID
    var pgguid : String ="null"
    var created_by: String ="null"
    var created_date: String ="null"
    var last_modified_by: String ="null"
    var last_modified_date: String ="null"
    var pgid: String ="null"
    var yhmc: String ="null"
    var yddh: String ="null"
    var yddh2: String ="null"
    var quhao: String ="null"
    var dhhm: String ="null"
    var fjhm: String ="null"
    var email: String ="null"
    var sfen: String ="null"
    var cshi: String ="null"
    var xian: String ="null"
    var xzhen: String ="null"
    var dizi: String ="null"
    var xxqd: String ="null"
    var xxly: String ="null"
    var xxlb: String ="null"
    var beiz: String ="null"
    var yhsx: String ="null"
    var yxji: String ="null"
    var gdhao: String ="null"
    var spid: String ="null"
    var spmc: String ="null"
    var azren: String ="null"
    var azrenid: String ="null"
    var azwdxtbh: String ="null"
    var azwdno: String ="null"
    var azwdmc: String ="null"
    var jspgwdno: String ="null"
    var jspgwdmc: String ="null"
    var jspgwdxtbh: String ="null"
    var jspgwdsj: String ="null"
    var zxha: String ="null"
    var ssqy: String ="null"
    var qqlyno: String ="null"
    var qqlymc: String ="null"
    var qqlyxh: String ="null"
    var qqlyzj: String ="null"
    var bjustat: String ="null"
    var yhqwkssj: String ="null"
    var yhqwjssj: String ="null"
    var stat: String ="null"
    var gpsdzxx: String ="null"
    var cjren: String ="null"
    var cjrmc: String ="null"
    var cjdt: String ="null"
    var cjwdno: String ="null"
    var cjwdxtbh: String ="null"
    var zjczren: String ="null"
    var zjczwd: String ="null"
    var zjczwdxtbh: String ="null"
    var zjczsj: String ="null"
    var xslx: String ="null"
    var lcid: String ="null"
    var djlxno: String ="null"
    var yyazsj: String ="null"
    var sfwcps: String ="null"
    var xsdh: String ="null"
    var gcbh: String ="null"
    var gcmc: String ="null"
    var azsl: String ="null"
    var wwsl: String ="null"
    var lxren: String ="null"
    var xsdwno: String ="null"
    var xswdmc: String ="null"
    var xsdwxtbh: String ="null"
    var fphm: String ="null"
    var gmsj: String ="null"
    var kqbh: String ="null"
    var xsorsh: String ="null"
    var dqjdsj: String ="null"
    var syjd: String ="null"
    var dqjd: String ="null"
    var jindu: String ="null"
    var weidu: String ="null"
    var shsj: String ="null"
    var sfygllc: String ="null"
    var xslxid: String ="null"
    var yhsxid: String ="null"
    var xxlbid: String ="null"
    var xxlyid: String ="null"
    var sfenid: String ="null"
    var cshiid: String ="null"
    var xianid: String ="null"
    var xzhenid: String ="null"
    var wcsj: String ="null"
    var retailsign: String = "null"
    var servicewdmc: String = "null"
    var shopname: String = "null"
    var orderphone: String = "null"
    var extendfiled1: String = "null"
    var extendfiled2: String = "null"
    var extendfiled3: String = "null"
    var extendfiled4: String = "null"
    var servicewdno: String = "null"
    var shopno: String = "null"
    var extendfiled5: String = "null"
    var organizationaddress: String = "null"
    var organizationname: String = "null"
    var appointmentkssj: String = "null"
    var appointmentjssj: String = "null"
    var platformBizorderid : String = "null"
    var fkmxLastestMessage : String = "null"
    val client: TransportClient = ESTransportPoolUtil.getClient


    //查主表
    val zhuBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_AZ_ASSIGN_LC_LS)//default_server_greeshinstall_tbl_az_assign_lc_ls_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.termQuery("pgguid", value._1))
      .get()

    val hits: SearchHits = zhuBiao.getHits

    if (hits.iterator().hasNext) {
      val hit = hits.iterator().next()
      logger.info("反查安装主表的结果为->" + hit.getSourceAsString)
      try {
        pgguid= hit.getSourceAsMap.get("pgguid").toString
        created_by= hit.getSourceAsMap.get("created_by").toString
        created_date= hit.getSourceAsMap.get("created_date").toString
        last_modified_by= hit.getSourceAsMap.get("last_modified_by").toString
        last_modified_date= hit.getSourceAsMap.get("last_modified_date").toString
        pgid= hit.getSourceAsMap.get("pgid").toString
        yhmc= hit.getSourceAsMap.get("yhmc").toString
        yddh= hit.getSourceAsMap.get("yddh").toString
        yddh2= hit.getSourceAsMap.get("yddh2").toString
        quhao= hit.getSourceAsMap.get("quhao").toString
        dhhm= hit.getSourceAsMap.get("dhhm").toString
        fjhm= hit.getSourceAsMap.get("fjhm").toString
        email= hit.getSourceAsMap.get("email").toString
        sfen= hit.getSourceAsMap.get("sfen").toString
        cshi= hit.getSourceAsMap.get("cshi").toString
        xian= hit.getSourceAsMap.get("xian").toString
        xzhen= hit.getSourceAsMap.get("xzhen").toString
        dizi= hit.getSourceAsMap.get("dizi").toString
        xxqd= hit.getSourceAsMap.get("xxqd").toString
        xxly= hit.getSourceAsMap.get("xxly").toString
        xxlb= hit.getSourceAsMap.get("xxlb").toString
        beiz= hit.getSourceAsMap.get("beiz").toString
        yhsx= hit.getSourceAsMap.get("yhsx").toString
        yxji= hit.getSourceAsMap.get("yxji").toString
        gdhao= hit.getSourceAsMap.get("gdhao").toString
        spid= hit.getSourceAsMap.get("spid").toString
        spmc= hit.getSourceAsMap.get("spmc").toString
        azren= hit.getSourceAsMap.get("azren").toString
        azrenid= hit.getSourceAsMap.get("azrenid").toString
        azwdxtbh= hit.getSourceAsMap.get("azwdxtbh").toString
        azwdno= hit.getSourceAsMap.get("azwdno").toString
        azwdmc= hit.getSourceAsMap.get("azwdmc").toString
        jspgwdno= hit.getSourceAsMap.get("jspgwdno").toString
        jspgwdmc= hit.getSourceAsMap.get("jspgwdmc").toString
        jspgwdxtbh= hit.getSourceAsMap.get("jspgwdxtbh").toString
        jspgwdsj= hit.getSourceAsMap.get("jspgwdsj").toString
        zxha= hit.getSourceAsMap.get("zxha").toString
        ssqy= hit.getSourceAsMap.get("ssqy").toString
        qqlyno= hit.getSourceAsMap.get("qqlyno").toString
        qqlymc= hit.getSourceAsMap.get("qqlymc").toString
        qqlyxh= hit.getSourceAsMap.get("qqlyxh").toString
        qqlyzj= hit.getSourceAsMap.get("qqlyzj").toString
        bjustat= hit.getSourceAsMap.get("bjustat").toString
        yhqwkssj= hit.getSourceAsMap.get("yhqwkssj").toString
        yhqwjssj= hit.getSourceAsMap.get("yhqwjssj").toString
        stat= hit.getSourceAsMap.get("stat").toString
        gpsdzxx= hit.getSourceAsMap.get("gpsdzxx").toString
        cjren= hit.getSourceAsMap.get("cjren").toString
        cjrmc= hit.getSourceAsMap.get("cjrmc").toString
        cjdt= hit.getSourceAsMap.get("cjdt").toString
        cjwdno= hit.getSourceAsMap.get("cjwdno").toString
        cjwdxtbh= hit.getSourceAsMap.get("cjwdxtbh").toString
        zjczren= hit.getSourceAsMap.get("zjczren").toString
        zjczwd= hit.getSourceAsMap.get("zjczwd").toString
        zjczwdxtbh= hit.getSourceAsMap.get("zjczwdxtbh").toString
        zjczsj= hit.getSourceAsMap.get("zjczsj").toString
        xslx= hit.getSourceAsMap.get("xslx").toString
        lcid= hit.getSourceAsMap.get("lcid").toString
        djlxno= hit.getSourceAsMap.get("djlxno").toString
        yyazsj= hit.getSourceAsMap.get("yyazsj").toString
        sfwcps= hit.getSourceAsMap.get("sfwcps").toString
        xsdh= hit.getSourceAsMap.get("xsdh").toString
        gcbh= hit.getSourceAsMap.get("gcbh").toString
        gcmc= hit.getSourceAsMap.get("gcmc").toString
        azsl= hit.getSourceAsMap.get("azsl").toString
        wwsl= hit.getSourceAsMap.get("wwsl").toString
        lxren= hit.getSourceAsMap.get("lxren").toString
        xsdwno= hit.getSourceAsMap.get("xsdwno").toString
        xswdmc= hit.getSourceAsMap.get("xswdmc").toString
        xsdwxtbh= hit.getSourceAsMap.get("xsdwxtbh").toString
        fphm= hit.getSourceAsMap.get("fphm").toString
        gmsj= hit.getSourceAsMap.get("gmsj").toString
        kqbh= hit.getSourceAsMap.get("kqbh").toString
        xsorsh= hit.getSourceAsMap.get("xsorsh").toString
        dqjdsj= hit.getSourceAsMap.get("dqjdsj").toString
        syjd= hit.getSourceAsMap.get("syjd").toString
        dqjd= hit.getSourceAsMap.get("dqjd").toString
        jindu= hit.getSourceAsMap.get("jindu").toString
        weidu= hit.getSourceAsMap.get("weidu").toString
        shsj= hit.getSourceAsMap.get("shsj").toString
        sfygllc= hit.getSourceAsMap.get("sfygllc").toString
        xslxid= hit.getSourceAsMap.get("xslxid").toString
        yhsxid= hit.getSourceAsMap.get("yhsxid").toString
        xxlbid= hit.getSourceAsMap.get("xxlbid").toString
        xxlyid= hit.getSourceAsMap.get("xxlyid").toString
        sfenid= hit.getSourceAsMap.get("sfenid").toString
        cshiid= hit.getSourceAsMap.get("cshiid").toString
        xianid= hit.getSourceAsMap.get("xianid").toString
        xzhenid= hit.getSourceAsMap.get("xzhenid").toString
        wcsj = hit.getSourceAsMap.get("wcsj").toString
        retailsign = hit.getSourceAsMap.get("retailsign").toString
        servicewdmc = hit.getSourceAsMap.get("servicewdmc").toString
        shopname = hit.getSourceAsMap.get("shopname").toString
        orderphone = hit.getSourceAsMap.get("orderphone").toString
        extendfiled1 = hit.getSourceAsMap.get("extendfiled1").toString
        extendfiled2 = hit.getSourceAsMap.get("extendfiled2").toString
        extendfiled3 = hit.getSourceAsMap.get("extendfiled3").toString
        extendfiled4 = hit.getSourceAsMap.get("extendfiled4").toString
        servicewdno = hit.getSourceAsMap.get("servicewdno").toString
        shopno = hit.getSourceAsMap.get("shopno").toString
        extendfiled5 = hit.getSourceAsMap.get("extendfiled5").toString
        organizationaddress = hit.getSourceAsMap.get("organizationaddress").toString
        organizationname = hit.getSourceAsMap.get("organizationname").toString

        var level1:String = "null"
        var level2:String = "null"
        var level3:String = "null"
        var level4:String = "null"
        var level5:String = "null"
        var level6:String = "null"
        var level7:String = "null"

    if (!"null".equals(spid)){

      //从quanxianlevel_wangdianlevel_v1 ES中查出对应字段
      val quanXianLevel: SearchResponse = client
        .prepareSearch(Constant.WANGDIANL_EVEL)//quanxianlevel_wangdianlevel_v1
        .setTypes("_doc")
        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("stat", 2)).must(QueryBuilders.termQuery("fwlb", "售后"))
          .must(QueryBuilders.termQuery("wdno", jspgwdno)).must(QueryBuilders.termQuery("splb", Integer.valueOf(spid))))
        .get()

      if(quanXianLevel.getHits.iterator().hasNext){
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
          case e:Exception => logger.error("MaxAssignLclsFunction查询quanxianlevel_wangdianlevel_v1 ES表异常"+e.getMessage)
        }
      }
    }

    //从quanxianlevel_wangdianlevel_v1 ES中查出对应字段
    val quanXianLevelSale: SearchResponse = client
      .prepareSearch(Constant.WANGDIANL_EVEL)//quanxianlevel_wangdianlevel_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("stat", 2)).must(QueryBuilders.termQuery("fwlb", "销售"))
        .must(QueryBuilders.termQuery("wdno", cjwdno)).must(QueryBuilders.termQuery("splb", Integer.valueOf(spid))))
      .get()

    var level8:String = "null"
    var level9:String = "null"
    var level10:String = "null"
    var level11:String = "null"
    var level12:String = "null"
    var level13:String = "null"
    var level14:String = "null"

    if(quanXianLevelSale.getHits.iterator().hasNext){
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
        case e:Exception => logger.error("MaxAssignLclsFunction查询quanxianlevel_wangdianlevel_v1销售 ES表异常"+e.getMessage)
      }
    }

    //查预约表数据的最新的预约时间
    val YuYBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_AZ_ASSIGN_APPOINTMENT)//default_server_greeshinstall_tbl_az_assign_appointment_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.termQuery("pgguid", value._1))
      .addSort("czsj", SortOrder.DESC)
      .setFrom(0)
      .setSize(1)
      .get()

    val YuYBiaoHits: SearchHits = YuYBiao.getHits
    logger.info("MaxAssignLclsFunction查预约表总条数为->" + YuYBiaoHits.totalHits)

    //判断是否有预约时间
    if (YuYBiaoHits.iterator().hasNext) {

      val hit = YuYBiaoHits.iterator().next()
      logger.info("对应结果为->" + hit.getSourceAsString)
      try {
        appointmentkssj = hit.getSourceAsMap.get("kssj").toString
        appointmentjssj = hit.getSourceAsMap.get("jssj").toString
      } catch {
        case e: Exception => logger.error("MaxAssignLclsFunction从预约表查询异常->" + e.getMessage)
      }
    }

    //要求完工时间
    var yqwangongtime: String = "null"
    //如果预约时间为空，则要求完工时间为创建时间加一天
    if ("null".equals(appointmentkssj)) {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val parse = simpleDateFormat.parse(cjdt)
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


    //查询其他表
    //查询预约表
    val yuYueBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_AZ_ASSIGN_APPOINTMENT)//default_server_greeshinstall_tbl_az_assign_appointment_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.termQuery("pgguid", value._1))
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
        yuYueMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
        yuYueMap.put("czsj",hit.getSourceAsMap.get("czsj").toString)
        yuYueMap.put("leix",hit.getSourceAsMap.get("leix").toString)
        yuYueMap.put("reason",hit.getSourceAsMap.get("reason").toString)
        yuYueMap.put("beiz",hit.getSourceAsMap.get("beiz").toString)
        yuYueList.add(yuYueMap)


      } catch {
        case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_az_assign_appointment_v1异常"+e.getMessage)
      }
    }


    //查询Tbl_Az_Assign_Mx
    val mxBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_AZ_MX)//default_server_greeshinstall_tbl_az_assign_mx_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.termQuery("pgguid", value._1)) //直接查询即可,只对应一条
      .get()

    val mxList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()

    val mxListhits: SearchHits = mxBiao.getHits
    for (hit <- mxListhits){
      try {
        val mxMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        mxMap.put("pgmxid",hit.getSourceAsMap.get("pgmxid").toString)
        mxMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        mxMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        mxMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        mxMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        mxMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
        mxMap.put("spid",hit.getSourceAsMap.get("spid").toString)
        mxMap.put("spmc",hit.getSourceAsMap.get("spmc").toString)
        mxMap.put("xlid",hit.getSourceAsMap.get("xlid").toString)
        mxMap.put("xlmc",hit.getSourceAsMap.get("xlmc").toString)
        mxMap.put("xiid",hit.getSourceAsMap.get("xiid").toString)
        mxMap.put("ximc",hit.getSourceAsMap.get("ximc").toString)
        mxMap.put("jxmc",hit.getSourceAsMap.get("jxmc").toString)
        mxMap.put("jxno",hit.getSourceAsMap.get("jxno").toString)
        mxMap.put("czren",hit.getSourceAsMap.get("czren").toString)
        mxMap.put("czsj",hit.getSourceAsMap.get("czsj").toString)
        mxMap.put("czwd",hit.getSourceAsMap.get("czwd").toString)
        mxMap.put("njtm",hit.getSourceAsMap.get("njtm").toString)
        mxMap.put("wjtm",hit.getSourceAsMap.get("wjtm").toString)
        mxMap.put("beiz",hit.getSourceAsMap.get("beiz").toString)
        mxMap.put("shul",hit.getSourceAsMap.get("shul").toString)
        mxMap.put("cjdt",hit.getSourceAsMap.get("cjdt").toString)
        mxMap.put("jiage",hit.getSourceAsMap.get("jiage").toString)
        mxMap.put("danw",hit.getSourceAsMap.get("danw").toString)
        mxMap.put("wldm",hit.getSourceAsMap.get("wldm").toString)
        mxMap.put("njtm2",hit.getSourceAsMap.get("njtm2").toString)
        mxMap.put("wjsl",hit.getSourceAsMap.get("wjsl").toString)
        mxMap.put("njsl",hit.getSourceAsMap.get("njsl").toString)
        mxMap.put("wwsl",hit.getSourceAsMap.get("wwsl").toString)
        mxList.add(mxMap)


      } catch {
        case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_az_assign_mx_v1异常"+e.getMessage)
      }
    }


    //查询
    //
    val fkmxBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_AZ_ASSIGN_FKMX)//default_server_greeshinstall_tbl_az_assign_fkmx_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.termQuery("pgguid", value._1)) //直接查询即可,只对应一条
      .get()

    val fkmxList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()


    val fkmxListhits: SearchHits = fkmxBiao.getHits
    for (hit <- fkmxListhits){
      try {
        val fkmxMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        fkmxMap.put("fkid",hit.getSourceAsMap.get("fkid").toString)
        fkmxMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        fkmxMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        fkmxMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        fkmxMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        fkmxMap.put("fklb",hit.getSourceAsMap.get("fklb").toString)
        fkmxMap.put("fkjg",hit.getSourceAsMap.get("fkjg").toString)
        fkmxMap.put("fknr",hit.getSourceAsMap.get("fknr").toString)
        fkmxMap.put("fkren",hit.getSourceAsMap.get("fkren").toString)
        fkmxMap.put("fkrenmc",hit.getSourceAsMap.get("fkrenmc").toString)
        fkmxMap.put("fksj",hit.getSourceAsMap.get("fksj").toString)
        fkmxMap.put("xtwdbh",hit.getSourceAsMap.get("xtwdbh").toString)
        fkmxMap.put("wdno",hit.getSourceAsMap.get("wdno").toString)
        fkmxMap.put("wdmc",hit.getSourceAsMap.get("wdmc").toString)
        fkmxMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
        fkmxMap.put("cjdt",hit.getSourceAsMap.get("cjdt").toString)
        fkmxList.add(fkmxMap)

      } catch {
        case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_az_assign_fkmx_v1异常"+e.getMessage)
      }
    }

    //查询
    //
    val SatisfactionBiao: SearchResponse = client
      .prepareSearch(Constant.TBL_AZ_SATISFACTION)//default_server_greeshinstall_tbl_az_assign_satisfaction_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.termQuery("pgguid", value._1)) //直接查询即可,只对应一条
      .get()

    val manYiList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()


    val satisfactionhits: SearchHits = SatisfactionBiao.getHits
    for (hit <- satisfactionhits){
      try {
        val manYiMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        manYiMap.put("id",hit.getSourceAsMap.get("id").toString)
        manYiMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
        manYiMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
        manYiMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
        manYiMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
        manYiMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
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
        case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_az_assign_satisfaction_v1异常"+e.getMessage)
      }

    }

        //查询default_server_greeshinstall_tbl_az_assign_fee_v1
        val assignFeeBiao: SearchResponse = client
          .prepareSearch(Constant.TBL_AZ_ASSIGN_FEE)//"default_server_greeshinstall_tbl_az_assign_fee_v1"
          .setTypes("_doc")
          .setQuery(QueryBuilders.termQuery("pgguid", value._1)) //直接查询即可,只对应一条
          .get()

        val assignFeehits: SearchHits = assignFeeBiao.getHits

        val assignFeeList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()
        for (hit <- assignFeehits){
          try {
            val assignFeeMap: util.HashMap[String, String] = new util.HashMap[String,String]()
            assignFeeMap.put("id",hit.getSourceAsMap.get("id").toString)
            assignFeeMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
            assignFeeMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
            assignFeeMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
            assignFeeMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
            assignFeeMap.put("otherfee",hit.getSourceAsMap.get("otherfee").toString)
            assignFeeMap.put("totalfee",hit.getSourceAsMap.get("totalfee").toString)
            assignFeeMap.put("ajia",hit.getSourceAsMap.get("ajia").toString)
            assignFeeMap.put("jcguan",hit.getSourceAsMap.get("jcguan").toString)
            assignFeeMap.put("kqkg",hit.getSourceAsMap.get("kqkg").toString)
            assignFeeMap.put("gkzy",hit.getSourceAsMap.get("gkzy").toString)
            assignFeeMap.put("yccxqk",hit.getSourceAsMap.get("yccxqk").toString)
            assignFeeMap.put("flbz",hit.getSourceAsMap.get("flbz").toString)
            assignFeeMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
            assignFeeList.add(assignFeeMap)

          } catch {
            case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_az_assign_fee_v1异常"+e.getMessage)
          }
        }

        //查询default_server_greeshinstall_tbl_az_assign_lc_fzry_v1
        val lcFzryBiao: SearchResponse = client.prepareSearch(Constant.TBL_AZ_ASSIGN_LC_FZRY)//"default_server_greeshinstall_tbl_az_assign_lc_fzry_v1"
          .setTypes("_doc")
          .setQuery(QueryBuilders.termQuery("pgguid", value._1)) //直接查询即可,只对应一条
          .get()

        val lcFzryhits: SearchHits = lcFzryBiao.getHits
        val lcFzryList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()
        for (hit <- lcFzryhits){
          try {
            val lcFzryMap: util.HashMap[String, String] = new util.HashMap[String,String]()
            lcFzryMap.put("id",hit.getSourceAsMap.get("id").toString)
            lcFzryMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
            lcFzryMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
            lcFzryMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
            lcFzryMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
            lcFzryMap.put("azren",hit.getSourceAsMap.get("azren").toString)
            lcFzryMap.put("azrenid",hit.getSourceAsMap.get("azrenid").toString)
            lcFzryMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
            lcFzryList.add(lcFzryMap)

          } catch {
            case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_az_assign_lc_fzry_v1异常"+e.getMessage)
          }
        }

        //查询default_server_greeshinstall_tbl_trade_new_for_old_v1
        val newForOldBiao: SearchResponse = client.prepareSearch(Constant.TBL_TRADE_NEW_FOR_OLD)//"default_server_greeshinstall_tbl_trade_new_for_old_v1"
          .setTypes("_doc")
          .setQuery(QueryBuilders.termQuery("pgguid", value._1)) //直接查询即可,只对应一条
          .get()

        val newForOldhits: SearchHits = newForOldBiao.getHits
        val newForOldList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()
        for (hit <- newForOldhits){
          try {
            val newForOldMap: util.HashMap[String, String] = new util.HashMap[String,String]()
            newForOldMap.put("id",hit.getSourceAsMap.get("id").toString)
            newForOldMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
            newForOldMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
            newForOldMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
            newForOldMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
            newForOldMap.put("oldMachineBrand",hit.getSourceAsMap.get("oldMachineBrand").toString)
            newForOldMap.put("oldMachineType",hit.getSourceAsMap.get("oldMachineType").toString)
            newForOldMap.put("oldMachineNum",hit.getSourceAsMap.get("oldMachineNum").toString)
            newForOldMap.put("connector",hit.getSourceAsMap.get("connector").toString)
            newForOldMap.put("connectWay",hit.getSourceAsMap.get("connectWay").toString)
            newForOldMap.put("hxmxid",hit.getSourceAsMap.get("hxmxid").toString)
            newForOldMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
            newForOldMap.put("xsdh",hit.getSourceAsMap.get("xsdh").toString)
            newForOldMap.put("orderitemid",hit.getSourceAsMap.get("orderitemid").toString)
            newForOldMap.put("lcid",hit.getSourceAsMap.get("lcid").toString)
            newForOldList.add(newForOldMap)

          } catch {
            case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_trade_new_for_old_v1异常"+e.getMessage)
          }
        }

        //查询default_server_greeshinstall_tbl_yjhx_jdd_v1
        val yjhxJddBiao: SearchResponse = client.prepareSearch(Constant.TBL_YJHX_JDD)//"default_server_greeshinstall_tbl_yjhx_jdd_v1"
          .setTypes("_doc")
          .setQuery(QueryBuilders.termQuery("pgguid", value._1)) //直接查询即可,只对应一条
          .get()
        val yjhxJddhits = yjhxJddBiao.getHits
        val yjhxJddList: util.ArrayList[util.HashMap[String,String]] = new util.ArrayList[util.HashMap[String,String]]()
        for (hit <- yjhxJddhits){
          try {
            val yjhxJddMap: util.HashMap[String, String] = new util.HashMap[String,String]()
            yjhxJddMap.put("id",hit.getSourceAsMap.get("id").toString)
            yjhxJddMap.put("created_by",hit.getSourceAsMap.get("created_by").toString)
            yjhxJddMap.put("created_date",hit.getSourceAsMap.get("created_date").toString)
            yjhxJddMap.put("last_modified_by",hit.getSourceAsMap.get("last_modified_by").toString)
            yjhxJddMap.put("last_modified_date",hit.getSourceAsMap.get("last_modified_date").toString)
            yjhxJddMap.put("oldMachineBrand",hit.getSourceAsMap.get("oldMachineBrand").toString)
            yjhxJddMap.put("oldMachineType",hit.getSourceAsMap.get("oldMachineType").toString)
            yjhxJddMap.put("brandFlag",hit.getSourceAsMap.get("brandFlag").toString)
            yjhxJddMap.put("typeFlag",hit.getSourceAsMap.get("typeFlag").toString)
            yjhxJddMap.put("realBrand",hit.getSourceAsMap.get("realBrand").toString)
            yjhxJddMap.put("realType",hit.getSourceAsMap.get("realType").toString)
            yjhxJddMap.put("machineIntegrity",hit.getSourceAsMap.get("machineIntegrity").toString)
            yjhxJddMap.put("tempBarcode",hit.getSourceAsMap.get("tempBarcode").toString)
            yjhxJddMap.put("tempBarcodeImg",hit.getSourceAsMap.get("tempBarcodeImg").toString)
            yjhxJddMap.put("identifyResult",hit.getSourceAsMap.get("identifyResult").toString)
            yjhxJddMap.put("hxjddid",hit.getSourceAsMap.get("hxjddid").toString)
            yjhxJddMap.put("pgguid",hit.getSourceAsMap.get("pgguid").toString)
            yjhxJddMap.put("xsdh",hit.getSourceAsMap.get("xsdh").toString)
            yjhxJddMap.put("cpps",hit.getSourceAsMap.get("cpps").toString)
            yjhxJddList.add(yjhxJddMap)

          } catch {
            case e:Exception => logger.error("MaxAssignLclsFunction查询default_server_greeshinstall_tbl_yjhx_jdd_v1异常"+e.getMessage)
          }
        }

        //查询default_server_greeshinstall_tbl_az_assign_platform_v1
        val platformBiao: SearchResponse = client.prepareSearch(Constant.TBL_AZ_ASSIGN_PLATFORM)
          .setTypes("_doc")
          .setQuery(QueryBuilders.termQuery("pgguid", value._1))
          .get()
        val platformBiaoHits = platformBiao.getHits
        if (platformBiaoHits.iterator().hasNext){
          val hit = platformBiaoHits.iterator().next()
          platformBizorderid = hit.getSourceAsMap.get("bizorderid").toString
        }

        //查询
        //Tbl_Az_Assign_Fkmx返回最新一条
        val fkmxNewRecord: SearchResponse = client
          .prepareSearch(Constant.TBL_AZ_ASSIGN_FKMX)//"default_server_greeshinstall_tbl_az_assign_fkmx_v1"
          .setTypes("_doc")
          .setQuery(QueryBuilders.termQuery("pgguid", value._1))
          .addSort("cjdt",SortOrder.DESC)
          .setFrom(0)
          .setSize(1)
          .get()

        val fkmxNewRecordHits: SearchHits = fkmxNewRecord.getHits

        if (fkmxNewRecordHits.iterator().hasNext){
          try {
            fkmxLastestMessage = fkmxNewRecordHits.iterator().next().getSourceAsMap.get("fknr").toString
          } catch {
            case e:Exception =>logger.error("MaxAssignLclsFunction从fkmx表查询最新一条fknr异常"+e.getMessage)
          }
        }

    //输出
    try {
      out.collect(AZMaxOrder(pgguid, created_by, created_date, last_modified_by, last_modified_date, pgid, yhmc, yddh
        , yddh2, quhao, dhhm, fjhm, email, sfen, cshi, xian, xzhen, dizi, xxqd, xxly, xxlb, beiz, yhsx
        , yxji, gdhao, spid, spmc, azren, azrenid, azwdxtbh, azwdno, azwdmc, jspgwdno, jspgwdmc, jspgwdxtbh, jspgwdsj, zxha, ssqy, qqlyno, qqlymc
        , qqlyxh, qqlyzj, bjustat, yhqwkssj, yhqwjssj, stat, gpsdzxx, cjren, cjrmc, cjdt, cjwdno, cjwdxtbh, zjczren, zjczwd, zjczwdxtbh, zjczsj, xslx
        , lcid, djlxno, yyazsj, sfwcps, xsdh, gcbh, gcmc, azsl, wwsl, lxren, xsdwno, xswdmc, xsdwxtbh, fphm, gmsj, kqbh, xsorsh, dqjdsj, syjd, dqjd
        , jindu, weidu, shsj, sfygllc, xslxid, yhsxid, xxlbid, xxlyid, sfenid, cshiid, xianid, xzhenid, wcsj,retailsign,servicewdmc,shopname,
        orderphone,extendfiled1,extendfiled2,extendfiled3,extendfiled4,servicewdno,shopno,extendfiled5,
        appointmentkssj,
        appointmentjssj ,
        fkmxList,
        mxList,
        manYiList,
        yuYueList,
        assignFeeList,
        lcFzryList,
        newForOldList,
        yjhxJddList,
        level1,
        level2,
        level3,
        level4,
        level5,
        level6,
        level7,level8,level9,level10,level11,level12,level13,level14,yqwangongtime,organizationaddress,organizationname,
        platformBizorderid,fkmxLastestMessage
      ))
    } catch {
      case e:Exception =>logger.error("MaxAssignLclsFunction输出错误,原因"+e.getCause+"异常"+e.getMessage)
    }
      } catch {
        case e: Exception => logger.info("MaxAssignLclsFunction主表查询异常->" + e.getMessage)
      }
    }
    ESTransportPoolUtil.returnClient(client)
  }
}
