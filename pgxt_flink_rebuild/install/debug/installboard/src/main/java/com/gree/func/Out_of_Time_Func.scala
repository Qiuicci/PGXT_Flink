package com.gree.func

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.gree.constant.Constant
import com.gree.model.{AzData, AzDataChaoShi}
import com.gree.util.{EsConnection, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.{Logger, LoggerFactory}

class Out_of_Time_Func(chaoshi: OutputTag[AzDataChaoShi], weiwangongchaoshi: OutputTag[AzDataChaoShi], wangongweichaoshi: OutputTag[AzDataChaoShi]) extends ProcessFunction[(String,String,Long),AzData]{
  override def processElement(value: (String,String, Long), ctx: ProcessFunction[(String,String, Long), AzData]#Context, out: Collector[AzData]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(Out_of_Time_Func.super.getClass)
    val client: TransportClient = EsConnection.conn
    val util = new NumberFormatUtil
    var appointmentkssj: String = "null"
    var appointmentjssj: String = "null"
    var shijiwangongshijian: String = "1970-01-01 00:00:00"
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var timeout: String = "null"

    val response: SearchResponse = client
      .prepareSearch(Constant.TBL_AZ_ASSIGN_LC_LS) //default_server_greeshinstall_tbl_az_assign_lc_ls_v1
      .setTypes("_doc")
      .setQuery(QueryBuilders.termQuery("pgguid", value._1))
      .get()

    val hits: SearchHits = response.getHits

    var pgguid: String = "null"
    var created_by: String = "null"
    var created_date: String = "null"
    var last_modified_by: String = "null"
    var last_modified_date: String = "null"
    var pgid: String = "null"
    var yhmc: String = "null"
    var yddh: String = "null"
    var yddh2: String = "null"
    var quhao: String = "null"
    var dhhm: String = "null"
    var fjhm: String = "null"
    var email: String = "null"
    var sfen: String = "null"
    var cshi: String = "null"
    var xian: String = "null"
    var xzhen: String = "null"
    var dizi: String = "null"
    var xxqd: String = "null"
    var xxly: String = "null"
    var xxlb: String = "null"
    var beiz: String = "null"
    var yhsx: String = "null"
    var yxji: String = "null"
    var gdhao: String = "null"
    var spid: String = "null"
    var spmc: String = "null"
    var azren: String = "null"
    var azrenid: String = "null"
    var azwdxtbh: String = "null"
    var azwdno: String = "null"
    var azwdmc: String = "null"
    var cjdt: String = "null"
    var jspgwdno: String = "null"
    var jspgwdmc: String = "null"
    var jspgwdxtbh: String = "null"
    var jspgwdsj: String = "null"
    var zxha: String = "null"
    var ssqy: String = "null"
    var qqlyno: String = "null"
    var qqlymc: String = "null"
    var qqlyxh: String = "null"
    var qqlyzj: String = "null"
    var bjustat: String = "null"
    var yhqwkssj: String = "null"
    var yhqwjssj: String = "null"
    var stat: String = "null"
    var gpsdzxx: String = "null"
    var cjren: String = "null"
    var cjrmc: String = "null"
    var cjwdno: String = "null"
    var cjwdxtbh: String = "null"
    var zjczren: String = "null"
    var zjczwd: String = "null"
    var zjczwdxtbh: String = "null"
    var zjczsj: String = "null"
    var xslx: String = "null"
    var lcid: String = "null"
    var djlxno: String = "null"
    var yyazsj: String = "null"
    var sfwcps: String = "null"
    var xsdh: String = "null"
    var gcbh: String = "null"
    var gcmc: String = "null"
    var azsl: String = "null"
    var wwsl: String = "null"
    var lxren: String = "null"
    var xsdwno: String = "null"
    var xswdmc: String = "null"
    var xsdwxtbh: String = "null"
    var fphm: String = "null"
    var gmsj: String = "null"
    var kqbh: String = "null"
    var xsorsh: String = "null"
    var dqjdsj: String = "null"
    var syjd: String = "null"
    var dqjd: String = "null"
    var jindu: String = "null"
    var weidu: String = "null"
    var shsj: String = "null"
    var sfygllc: String = "null"
    var xslxid: String = "null"
    var yhsxid: String = "null"
    var xxlbid: String = "null"
    var xxlyid: String = "null"
    var sfenid: String = "null"
    var cshiid: String = "null"
    var xianid: String = "null"
    var xzhenid: String = "null"
    var wcsj: String = "null"
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

    if (hits.iterator().hasNext) {
      val hit = hits.iterator().next()
      logger.info("AnZhuangKanBanFunction2查主表对应结果为->" + hit.getSourceAsString)
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


      } catch {
        case e: Exception => logger.error("AnZhuangKanBanFunction2从主表查询异常->" + e.getMessage)
      }


      val zhuangtai: Int = util.panduanInt(dqjd)

      //写查询语句查出预约表需要的字段
      val yuYueBiao: SearchResponse = client
        .prepareSearch(Constant.TBL_AZ_ASSIGN_APPOINTMENT) //default_server_greeshinstall_tbl_az_assign_appointment_v1
        .setTypes("_doc")
        .setQuery(QueryBuilders.termQuery("pgguid", value._1))
        .addSort("czsj", SortOrder.DESC)
        .setFrom(0)
        .setSize(1)
        .get()

      val yuYueBiaoHits: SearchHits = yuYueBiao.getHits

      if (yuYueBiaoHits.iterator().hasNext) {

        val hit = yuYueBiaoHits.iterator().next()
        logger.info("AnZhuangKanBanFunction1查出预约表最新一条数据对应结果为->" + hit.getSourceAsString)
        try {
          appointmentkssj = hit.getSourceAsMap.get("kssj").toString
          appointmentjssj = hit.getSourceAsMap.get("jssj").toString
        } catch {
          case e: Exception => logger.error("AnZhuangKanBanFunction1从预约表查询异常->" + e.getMessage)
        }
      }

      //写查询语句查出反馈明细表，最新的
      val fanKuiMxBiao: SearchResponse = client
        .prepareSearch(Constant.TBL_AZ_ASSIGN_FKMX) //default_server_greeshinstall_tbl_az_assign_fkmx_v1
        .setTypes("_doc")
        .setQuery(QueryBuilders.termQuery("pgguid", value._1))
        .addSort("cjdt", SortOrder.DESC)
        .setFrom(0)
        .setSize(1)
        .get()

      val fanKuiMxBiaoHit: SearchHits = fanKuiMxBiao.getHits

      // 抓取反馈明细表中反馈类别字段后续根据该字段进行 是否为 驳回状态 筛选数据
      var cljggz: String = "null"
      if (fanKuiMxBiaoHit.iterator().hasNext) {
        val hit = fanKuiMxBiaoHit.iterator().next()
        logger.info("AnZhuangKanBanFunction1从fkmx表中查询最新一条数据结果为->" + hit.getSourceAsString)
        try {
          cljggz = hit.getSourceAsMap.get("fknr").toString
        } catch {
          case e: Exception => logger.error("AnZhuangKanBanFunction1从反馈明细表中查询反馈内容异常：" + e.getMessage)
        }
      }

      //根据消费主表数据中的pgguid查询反馈明细表（指定反馈类别 = 完工反馈）
      val fanKuiMxBiao_wgfk: SearchResponse = client
        .prepareSearch(Constant.TBL_AZ_ASSIGN_FKMX) //default_server_greeshinstall_tbl_az_assign_fkmx_v1
        .setTypes("_doc")
        .setQuery(QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery("pgguid", value._1))
          .must(QueryBuilders.termQuery("fklb", "完工")))
        .addSort("cjdt", SortOrder.ASC)
        .setFrom(0)
        .setSize(1)
        .get()
      val fanKuiMxBiao_wgfkHit: SearchHits = fanKuiMxBiao_wgfk.getHits

      if (fanKuiMxBiao_wgfkHit.iterator().hasNext) {
        shijiwangongshijian = fanKuiMxBiao_wgfkHit.iterator().next().getSourceAsMap.get("cjdt").toString
      }

      //从quanxianlevel_wangdianlevel_v1 ES中查出对应字段
      val quanXianLevel: SearchResponse = client
        .prepareSearch(Constant.WANGDIANL_EVEL) //quanxianlevel_wangdianlevel_v1
        .setTypes("_doc")
        .setQuery(QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery("stat", 2))
          .must(QueryBuilders.termQuery("fwlb", "售后"))
          .must(QueryBuilders.termQuery("wdno", jspgwdno))
          .must(QueryBuilders.termQuery("splb", spid)))
        .get()

      var level1: String = "null"
      var level2: String = "null"
      var level3: String = "null"
      var level4: String = "null"
      var level5: String = "null"
      var level6: String = "null"
      var level7: String = "null"

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
          case e: Exception => logger.error("AnZhuangKanBanFunction1查询quanxianlevel_wangdianlevel_v1 ES表异常" + e.getMessage)
        }
      }

      //从quanxianlevel_wangdianlevel_v1 ES中查出对应字段
      val quanXianLevelSale: SearchResponse = client
        .prepareSearch(Constant.WANGDIANL_EVEL) //quanxianlevel_wangdianlevel_v1
        .setTypes("_doc")
        .setQuery(QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery("stat", 2))
          .must(QueryBuilders.termQuery("fwlb", "销售"))
          .must(QueryBuilders.termQuery("wdno", cjwdno))
          .must(QueryBuilders.termQuery("splb", spid)))
        .get()

      var level8: String = "null"
      var level9: String = "null"
      var level10: String = "null"
      var level11: String = "null"
      var level12: String = "null"
      var level13: String = "null"
      var level14: String = "null"

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
          case e: Exception => logger.error("AnZhuangKanBanFunction1查询quanxianlevel_wangdianlevel_v1销售 ES表异常" + e.getMessage)
        }
      }


      //用户订单创建时间+24小时
      var cjdtParse = new Date()
      try {
        cjdtParse = simpleDateFormat.parse(cjdt)
      } catch {
        case e: Exception => println("获取创建时间异常：" + e.getMessage)
      }

      val cjdtCal = Calendar.getInstance
      cjdtCal.setTime(cjdtParse)
      cjdtCal.add(Calendar.DATE, 1)
      var YQwangongtime: String = simpleDateFormat.format(cjdtCal.getTime())

      //优先判别预约表的明细时间
      if (!"null".equals(appointmentkssj)) {
        val appointmentkssjParse = simpleDateFormat.parse(appointmentkssj)
        val appointmentkssjCal = Calendar.getInstance
        appointmentkssjCal.setTime(appointmentkssjParse)
        appointmentkssjCal.add(Calendar.DATE, 1)
        YQwangongtime = simpleDateFormat.format(appointmentkssjCal.getTime())
      }

      val YQwangongtimeParse = simpleDateFormat.parse(YQwangongtime)
      val YQwangongtimeCal = Calendar.getInstance
      YQwangongtimeCal.setTime(YQwangongtimeParse)

      if (zhuangtai >= 1304) {

        val wcsjParse = simpleDateFormat.parse(shijiwangongshijian)
        val wcsjCal = Calendar.getInstance
        wcsjCal.setTime(wcsjParse)


        //已完工从未完工的mysql删除
        ctx.output[AzDataChaoShi](wangongweichaoshi, AzDataChaoShi(
          pgguid, created_by, created_date, last_modified_by, last_modified_date,
          pgid, yhmc, yddh, yddh2, quhao, dhhm, fjhm, email,
          sfen, cshi, xian, xzhen, dizi, xxqd, xxly,
          xxlb, beiz, yhsx, yxji, gdhao, spid, spmc, azren,
          azrenid, azwdxtbh, azwdno, azwdmc, jspgwdno, jspgwdmc,
          jspgwdxtbh, jspgwdsj, zxha, ssqy, qqlyno, qqlymc, qqlyxh, qqlyzj,
          bjustat, yhqwkssj, yhqwjssj, stat, gpsdzxx, cjren, cjrmc, cjdt,
          cjwdno, cjwdxtbh, zjczren, zjczwd, zjczwdxtbh, zjczsj,
          xslx, lcid, djlxno, yyazsj, sfwcps, xsdh, gcbh,
          gcmc, azsl, wwsl, lxren, xsdwno, xswdmc,
          xsdwxtbh, fphm, gmsj, kqbh, xsorsh, dqjdsj, syjd,
          dqjd, jindu, weidu, shsj, sfygllc, xslxid,
          yhsxid, xxlbid, xxlyid, sfenid, cshiid, xianid,
          xzhenid, wcsj, retailsign, servicewdmc, shopname, orderphone, extendfiled1,
          extendfiled2, extendfiled3, extendfiled4, servicewdno, shopno, extendfiled5, cljggz, appointmentkssj, appointmentjssj, YQwangongtime, timeout, shijiwangongshijian, level1, level2, level3,
          level4, level5, level6, level7, level8, level9, level10, level11, level12, level13, level14,organizationaddress,organizationname))

        if (wcsjCal.compareTo(YQwangongtimeCal) > 0 && fanKuiMxBiao_wgfkHit.iterator().hasNext) {


          //超时时间
          timeout = ((wcsjCal.getTimeInMillis() - YQwangongtimeCal.getTimeInMillis()) / 3600d / 1000d).formatted("%.1f").toString


          ctx.output[AzDataChaoShi](chaoshi, AzDataChaoShi(
            pgguid, created_by, created_date, last_modified_by, last_modified_date,
            pgid, yhmc, yddh, yddh2, quhao, dhhm, fjhm, email,
            sfen, cshi, xian, xzhen, dizi, xxqd, xxly,
            xxlb, beiz, yhsx, yxji, gdhao, spid, spmc, azren,
            azrenid, azwdxtbh, azwdno, azwdmc, jspgwdno, jspgwdmc,
            jspgwdxtbh, jspgwdsj, zxha, ssqy, qqlyno, qqlymc, qqlyxh, qqlyzj,
            bjustat, yhqwkssj, yhqwjssj, stat, gpsdzxx, cjren, cjrmc, cjdt,
            cjwdno, cjwdxtbh, zjczren, zjczwd, zjczwdxtbh, zjczsj,
            xslx, lcid, djlxno, yyazsj, sfwcps, xsdh, gcbh,
            gcmc, azsl, wwsl, lxren, xsdwno, xswdmc,
            xsdwxtbh, fphm, gmsj, kqbh, xsorsh, dqjdsj, syjd,
            dqjd, jindu, weidu, shsj, sfygllc, xslxid,
            yhsxid, xxlbid, xxlyid, sfenid, cshiid, xianid,
            xzhenid, wcsj, retailsign, servicewdmc, shopname, orderphone, extendfiled1,
            extendfiled2, extendfiled3, extendfiled4, servicewdno, shopno, extendfiled5, cljggz,
            appointmentkssj, appointmentjssj, YQwangongtime, timeout, shijiwangongshijian, level1, level2, level3,
            level4, level5, level6, level7, level8, level9, level10, level11, level12, level13, level14,organizationaddress,organizationname))

        } else {
          try {
            //状态改变，从ES超时表中删除
            val ChaoShi: DeleteResponse = client
              .prepareDelete(Constant.AZKB_TIMEOUT_ORDER, "_doc", pgguid) //azkb_timeout_orders_v3
              .get()
            logger.info("AnZhuangKanBanFunction1从完工超时中删除数据id：" + ChaoShi.getId)
          } catch {
            case e: Exception => logger.error("AnZhuangKanBanFunction1从完工超时中删除数据异常：" + e.getMessage)
          }

        }
      } else {
        try {
          //状态改变，从ES超时表中删除
          val ChaoShi: DeleteResponse = client
            .prepareDelete(Constant.AZKB_TIMEOUT_ORDER, "_doc", pgguid) //azkb_timeout_orders_v3
            .get()
          logger.info("AnZhuangKanBanFunction1从完工超时中删除数据id：" + ChaoShi.getId)
        } catch {
          case e: Exception => logger.error("AnZhuangKanBanFunction1从完工超时中删除数据异常：" + e.getMessage)
        }


        //未完工未超时写入mysql
        ctx.output[AzDataChaoShi](weiwangongchaoshi, AzDataChaoShi(
          pgguid, created_by, created_date, last_modified_by, last_modified_date,
          pgid, yhmc, yddh, yddh2, quhao, dhhm, fjhm, email,
          sfen, cshi, xian, xzhen, dizi, xxqd, xxly,
          xxlb, beiz, yhsx, yxji, gdhao, spid, spmc, azren,
          azrenid, azwdxtbh, azwdno, azwdmc, jspgwdno, jspgwdmc,
          jspgwdxtbh, jspgwdsj, zxha, ssqy, qqlyno, qqlymc, qqlyxh, qqlyzj,
          bjustat, yhqwkssj, yhqwjssj, stat, gpsdzxx, cjren, cjrmc, cjdt,
          cjwdno, cjwdxtbh, zjczren, zjczwd, zjczwdxtbh, zjczsj,
          xslx, lcid, djlxno, yyazsj, sfwcps, xsdh, gcbh,
          gcmc, azsl, wwsl, lxren, xsdwno, xswdmc,
          xsdwxtbh, fphm, gmsj, kqbh, xsorsh, dqjdsj, syjd,
          dqjd, jindu, weidu, shsj, sfygllc, xslxid,
          yhsxid, xxlbid, xxlyid, sfenid, cshiid, xianid,
          xzhenid, wcsj, retailsign, servicewdmc, shopname, orderphone, extendfiled1,
          extendfiled2, extendfiled3, extendfiled4, servicewdno, shopno, extendfiled5, cljggz,
          appointmentkssj, appointmentjssj, YQwangongtime, timeout, shijiwangongshijian, level1, level2, level3,
          level4, level5, level6, level7, level8, level9, level10, level11, level12, level13, level14,organizationaddress,organizationname))
      }
    }
  }
}
