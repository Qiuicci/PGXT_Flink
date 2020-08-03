package com.gree.evaluate

import com.gree.model.{KuanBiaoData, WangdianLevel}
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

class UpdateWxMaxKuanBiao extends ProcessFunction[WangdianLevel,KuanBiaoData] {
  override def processElement(value: WangdianLevel, ctx: ProcessFunction[WangdianLevel, KuanBiaoData]#Context, out: Collector[KuanBiaoData]): Unit = {
    //ES链接
    val client: TransportClient = ESTransportPoolUtil.getClient
    val logger: Logger = LoggerFactory.getLogger(UpdateWxMaxKuanBiao.super.getClass)

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
    var assignMxSpid: String = "null"
    var assignMxSpmc: String = "null"
    var assignMxXlid: String = "null"
    var assignMxXlmc: String = "null"
    var assignMxXiid: String = "null"
    var assignMxXimc: String = "null"
    var assignMxJxid: String = "null"
    var assignMxJxmc: String = "null"
    var assignMxJxno: String = "null"
    var assignMxGmsj: String = "null"
    var assignMxXsdw: String = "null"
    var assignMxXsdwdh: String = "null"
    var assignMxFwdw: String = "null"
    var assignMxFwdwdh: String = "null"
    var assignMxGzwz: String = "null"
    var assignMxGzxx: String = "null"
    var assignMxCzren: String = "null"
    var assignMxCzsj: String = "null"
    var assignMxCzwd: String = "null"
    var assignMxNjtm: String = "null"
    var assignMxWjtm: String = "null"
    var assignMxBeiz: String = "null"
    var assignMxNjtm2: String = "null"
    var assignMxQqlyxh: String = "null"
    var assignMxPinpai: String = "null"
    var assignMxFee: String = "null"
    var assignMxXxfee: String = "null"
    var assignMxHsqk: String = "null"
    var assignMxGzxxid: String = "null"
    var assignMxShul: String = "null"
    var assignMxWwsl: String = "null"
    var assignFkmxFklb: String = "null"
    var assignFkmxFkjg: String = "null"
    var assignFkmxFknr: String = "null"
    var assignFkmxFkren: String = "null"
    var assignFkmxFkrenmc: String = "null"
    var assignFkmxFksj: String = "null"
    var assignFkmxFkwdno: String = "null"
    var assignFkmxFkwdmc: String = "null"
    var assignFkmxScid: String = "null"
    var assignFkmxScwj: String = "null"
    var assignFkmxQqlyxh: String = "null"
    var assignFkmxFkmxguid: String = "null"
    var assignFkmxWjid: String = "null"
    var assignFkmxDuanXinTime: String = "null"
    var assignSatisfactionPjly: String = "null"
    var assignSatisfactionPjnr: String = "null"
    var assignSatisfactionHfren: String = "null"
    var assignSatisfactionHfwdmc: String = "null"
    var assignSatisfactionHfwdno: String = "null"
    var assignSatisfactionHfsj: String = "null"
    var assignSatisfactionBmylx: String = "null"
    var assignSatisfactionBmybeiz: String = "null"
    var assignSatisfactionBmysj: String = "null"
    var assignSatisfactionSplb: String = "null"
    var assignSatisfactionMydlx: String = "null"
    var assignSatisfactionSxlx: String = "null"
    var assignFeedBackZlfksj: String = "null"
    var assignFeedBackZlfkbh: String = "null"
    var assignFeedBackCzren: String = "null"
    var assignFeedBackCzsj: String = "null"
    var yqwangongtime: String = "null"
    var chaoshitime: String = "null"
    var assignMxPgmxid: String = "null"
    var assignFkmxFkid: String = "null"
    var assignFeedBackId: String = "null"
    var assignSatisfactionId: String = "null"
    var shijiwangongtime: String = "null"
    var wangongfknr: String = "null"
    var appointmentkssj: String = "null"
    var appointmentjssj: String = "null"
    var zixinxishu: String = "null"


    val response: SearchResponse = client
      .prepareSearch("wx_export_data_all_orders_v3")
      .setTypes("_doc")
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("xjwdno", value.wdno))
        .must(QueryBuilders.termQuery("spid", value.splb))
        .must(QueryBuilders.rangeQuery("stat").lte(40)))
      .setSize(30000)
      .get()

    val hits: SearchHits = response.getHits
    logger.info("wx_export_data_all_orders_v3符合的数据有" + hits.totalHits + "条")

    for (hit <- hits) {

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
        wwsl = hit.getSourceAsMap.get("wwsl").toString
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
        appointmentkssj = hit.getSourceAsMap.get("appointmentkssj").toString
        appointmentjssj = hit.getSourceAsMap.get("appointmentjssj").toString
        assignFkmxFkid = hit.getSourceAsMap.get("assignFkmxFkid").toString
        assignFkmxFklb = hit.getSourceAsMap.get("assignFkmxFklb").toString
        assignFkmxFkjg = hit.getSourceAsMap.get("assignFkmxFkjg").toString
        assignFkmxFknr = hit.getSourceAsMap.get("assignFkmxFknr").toString
        assignFkmxFkren = hit.getSourceAsMap.get("assignFkmxFkren").toString
        assignFkmxFkrenmc = hit.getSourceAsMap.get("assignFkmxFkrenmc").toString
        assignFkmxFksj = hit.getSourceAsMap.get("assignFkmxFksj").toString
        assignFkmxFkwdno = hit.getSourceAsMap.get("assignFkmxFkwdno").toString
        assignFkmxFkwdmc = hit.getSourceAsMap.get("assignFkmxFkwdmc").toString
        assignFkmxScid = hit.getSourceAsMap.get("assignFkmxScid").toString
        assignFkmxScwj = hit.getSourceAsMap.get("assignFkmxScwj").toString
        assignFkmxQqlyxh = hit.getSourceAsMap.get("assignFkmxQqlyxh").toString
        assignFkmxFkmxguid = hit.getSourceAsMap.get("assignFkmxFkmxguid").toString
        assignFkmxWjid = hit.getSourceAsMap.get("assignFkmxWjid").toString
        assignFkmxDuanXinTime = hit.getSourceAsMap.get("assignFkmxDuanXinTime").toString
        assignMxPgmxid = hit.getSourceAsMap.get("assignMxPgmxid").toString
        assignMxSpid = hit.getSourceAsMap.get("assignMxSpid").toString
        assignMxSpmc = hit.getSourceAsMap.get("assignMxSpmc").toString
        assignMxXlid = hit.getSourceAsMap.get("assignMxXlid").toString
        assignMxXlmc = hit.getSourceAsMap.get("assignMxXlmc").toString
        assignMxXiid = hit.getSourceAsMap.get("assignMxXiid").toString
        assignMxXimc = hit.getSourceAsMap.get("assignMxXimc").toString
        assignMxJxid = hit.getSourceAsMap.get("assignMxJxid").toString
        assignMxJxmc = hit.getSourceAsMap.get("assignMxJxmc").toString
        assignMxJxno = hit.getSourceAsMap.get("assignMxJxno").toString
        assignMxGmsj = hit.getSourceAsMap.get("assignMxGmsj").toString
        assignMxXsdw = hit.getSourceAsMap.get("assignMxXsdw").toString
        assignMxXsdwdh = hit.getSourceAsMap.get("assignMxXsdwdh").toString
        assignMxFwdw = hit.getSourceAsMap.get("assignMxFwdw").toString
        assignMxFwdwdh = hit.getSourceAsMap.get("assignMxFwdwdh").toString
        assignMxGzwz = hit.getSourceAsMap.get("assignMxGzwz").toString
        assignMxGzxx = hit.getSourceAsMap.get("assignMxGzxx").toString
        assignMxCzren = hit.getSourceAsMap.get("assignMxCzren").toString
        assignMxCzsj = hit.getSourceAsMap.get("assignMxCzsj").toString
        assignMxCzwd = hit.getSourceAsMap.get("assignMxCzwd").toString
        assignMxNjtm = hit.getSourceAsMap.get("assignMxNjtm").toString
        assignMxWjtm = hit.getSourceAsMap.get("assignMxWjtm").toString
        assignMxBeiz = hit.getSourceAsMap.get("assignMxBeiz").toString
        assignMxNjtm2 = hit.getSourceAsMap.get("assignMxNjtm2").toString
        assignMxQqlyxh = hit.getSourceAsMap.get("assignMxQqlyxh").toString
        assignMxPinpai = hit.getSourceAsMap.get("assignMxPinpai").toString
        assignMxFee = hit.getSourceAsMap.get("assignMxFee").toString
        assignMxXxfee = hit.getSourceAsMap.get("assignMxXxfee").toString
        assignMxHsqk = hit.getSourceAsMap.get("assignMxHsqk").toString
        assignMxGzxxid = hit.getSourceAsMap.get("assignMxGzxxid").toString
        assignMxShul = hit.getSourceAsMap.get("assignMxShul").toString
        assignMxWwsl = hit.getSourceAsMap.get("assignMxWwsl").toString
        assignFeedBackId = hit.getSourceAsMap.get("assignFeedBackId").toString
        assignFeedBackZlfksj = hit.getSourceAsMap.get("assignFeedBackZlfksj").toString
        assignFeedBackZlfkbh = hit.getSourceAsMap.get("assignFeedBackZlfkbh").toString
        assignFeedBackCzren = hit.getSourceAsMap.get("assignFeedBackCzren").toString
        assignFeedBackCzsj = hit.getSourceAsMap.get("assignFeedBackCzsj").toString
        assignSatisfactionId = hit.getSourceAsMap.get("assignSatisfactionId").toString
        assignSatisfactionPjly = hit.getSourceAsMap.get("assignSatisfactionPjly").toString
        assignSatisfactionPjnr = hit.getSourceAsMap.get("assignSatisfactionPjnr").toString
        assignSatisfactionHfren = hit.getSourceAsMap.get("assignSatisfactionHfren").toString
        assignSatisfactionHfwdmc = hit.getSourceAsMap.get("assignSatisfactionHfwdmc").toString
        assignSatisfactionHfwdno = hit.getSourceAsMap.get("assignSatisfactionHfwdno").toString
        assignSatisfactionHfsj = hit.getSourceAsMap.get("assignSatisfactionHfsj").toString
        assignSatisfactionBmylx = hit.getSourceAsMap.get("assignSatisfactionBmylx").toString
        assignSatisfactionBmybeiz = hit.getSourceAsMap.get("assignSatisfactionBmybeiz").toString
        assignSatisfactionBmysj = hit.getSourceAsMap.get("assignSatisfactionBmysj").toString
        assignSatisfactionSplb = hit.getSourceAsMap.get("assignSatisfactionSplb").toString
        assignSatisfactionMydlx = hit.getSourceAsMap.get("assignSatisfactionMydlx").toString
        assignSatisfactionSxlx = hit.getSourceAsMap.get("assignSatisfactionSxlx").toString
        zixinxishu = hit.getSourceAsMap.get("zixinxishu").toString
        chaoshitime = hit.getSourceAsMap.get("chaoshitime").toString
        yqwangongtime = hit.getSourceAsMap.get("yqwangongtime").toString
        wangongfknr = hit.getSourceAsMap.get("wangongfknr").toString
        shijiwangongtime = hit.getSourceAsMap.get("shijiwangongtime").toString

      } catch {
        case e: Exception => logger.error("UpdateWxMaxKuanBiao查wx_export_data_all_orders_v3表异常->" + e.getMessage)
      }

      out.collect(KuanBiaoData(pgid, created_by, created_date, last_modified_by, last_modified_date, AUTH_STATE, azsl,
        beiz, bjustat, chaoshiqe, cjdt, cjren, cjrmc, cjwdno, cshi, cxyzm,
        dhhm, dizi, dqjdsj, email, fjhm, fwrybwgsj, gdhao, gpsdzxx, jindu, ldcs, pgguid, qqlymc, qqlyxh, qqlyzj,
        quhao, qwsmjssj, qystat, sfen, sffswx, spid, spmc, ssqy, stat, tsdengji, wcsj, weidu, wwsl, wxren, wxrenid,
        wxshul, wxwdmc, wxwdno, xian, xjwdmc, xjwdno, xjwdsj, xqxiaolei, xsdh, xsorsh, xswdmc, xswdno, xxlb, xxly,
        xxqd, xzhen, yddh, yddh2, yhgyhf, yhif, yhmc, yhqwsmsj, yhsx, yhyyczsj, yxji, zbby, zjczsj, zjczwd, zjczwdxtbh,
        zptype, zxhao, cshiid, sfenid, xianid, xxlbid, xxlyid, xxqdid, yhsxid, appointmentkssj,appointmentjssj, assignMxSpid, assignMxSpmc, assignMxXlid, assignMxXlmc, assignMxXlid, assignMxXimc, assignMxJxid,
        assignMxJxmc, assignMxJxno, assignMxGmsj, assignMxXsdw, assignMxXsdwdh, assignMxFwdw, assignMxFwdwdh, assignMxGzwz, assignMxGzxx, assignMxCzren, assignMxCzsj, assignMxCzwd, assignMxNjtm,
        assignMxWjtm, assignMxBeiz, assignMxNjtm2, assignMxQqlyxh, assignMxPinpai, assignMxFee, assignMxXxfee, assignMxHsqk, assignMxGzxxid, assignMxShul, assignMxWwsl, assignFkmxFklb, assignFkmxFkjg,
        assignFkmxFknr, assignFkmxFkren, assignFkmxFkrenmc, assignFkmxFksj, assignFkmxFkwdno, assignFkmxFkwdmc, assignFkmxScid, assignFkmxScwj, assignFkmxQqlyxh,
        assignFkmxFkmxguid, assignFkmxWjid, assignFkmxDuanXinTime, assignSatisfactionPjly, assignSatisfactionPjnr, assignSatisfactionHfren, assignSatisfactionHfwdmc,
        assignSatisfactionHfwdno, assignSatisfactionHfsj, assignSatisfactionBmylx, assignSatisfactionBmybeiz, assignSatisfactionBmysj, assignSatisfactionSplb,
        assignSatisfactionMydlx, assignSatisfactionSxlx, assignFeedBackZlfksj, assignFeedBackZlfkbh, assignFeedBackCzren, assignFeedBackCzsj,
        assignMxPgmxid, assignFkmxFkid, assignFeedBackId, assignSatisfactionId, zixinxishu, yqwangongtime, chaoshitime,wangongfknr,shijiwangongtime,
        value.first, value.second, value.third, value.fourth, value.fifth, value.sixth, value.seventh,"null","null","null","null","null","null","null"))
    }
    ESTransportPoolUtil.returnClient(client)
  }
}
