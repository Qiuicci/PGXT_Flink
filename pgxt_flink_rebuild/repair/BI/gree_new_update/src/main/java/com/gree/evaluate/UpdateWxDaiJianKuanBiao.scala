package com.gree.evaluate

import com.gree.model.{KuanBiaoDaiJianData, KuanBiaoData, WangdianLevel}
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

class UpdateWxDaiJianKuanBiao extends ProcessFunction[WangdianLevel,KuanBiaoDaiJianData] {
  override def processElement(value: WangdianLevel, ctx: ProcessFunction[WangdianLevel, KuanBiaoDaiJianData]#Context, out: Collector[KuanBiaoDaiJianData]): Unit = {
    //ES链接
    val client: TransportClient = ESTransportPoolUtil.getClient
    val logger: Logger = LoggerFactory.getLogger(UpdateWxDaiJianKuanBiao.super.getClass)

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
    var assignDaiJianid:String = "null"
    var assignDaiJianpjsqbh:String = "null"
    var assignDaiJianpjsqsj:String = "null"
    var assignDaiJianpjwlbm:String = "null"
    var assignDaiJianpjwlmc:String = "null"
    var assignDaiJianpjwlsl:String = "null"
    var assignDaiJiandjquyunum:String = "null"
    var assignDaiJiandjxsgsnum:String = "null"
    var assignDaiJiandjwdnum:String = "null"
    var assignDaiJiandjwd:String = "null"
    var assignDaiJiandjwdmc:String = "null"
    var assignDaiJiandjsj:String = "null"
    var assignDaiJiansplb:String = "null"
    var assignDaiJiancjdt:String = "null"
    var assignDaiJianthwlbm:String = "null"
    var assignDaiJianthwlmc:String = "null"
    var assignDaiJianthbmxsgsnum:String = "null"
    var assignDaiJianthbmqynum:String = "null"
    var assignDaiJianthbmwdnum:String = "null"
    var assignDaiJianpjxtflag:String = "null"
    var yqwangongtime: String = "null"
    var chaoshitime: String = "null"
    var shijiwangongtime: String = "null"
    var wangongfknr: String = "null"
    var appointmentkssj: String = "null"
    var appointmentjssj: String = "null"



    val response: SearchResponse = client
      .prepareSearch("wx_export_data_wait_part_orders_v3")
      .setTypes("_doc")
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("xjwdno", value.wdno))
        .must(QueryBuilders.termQuery("spid", value.splb))
        .must(QueryBuilders.rangeQuery("stat").lte(40)))
      .setSize(30000)
      .get()

    val hits: SearchHits = response.getHits
    logger.info("wx_export_data_wait_part_orders_v3符合的数据有" + hits.totalHits + "条")

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
        assignDaiJianid = hit.getSourceAsMap.get("assignDaiJianid").toString
        assignDaiJianpjsqbh = hit.getSourceAsMap.get("assignDaiJianpjsqbh").toString
        assignDaiJianpjsqsj = hit.getSourceAsMap.get("assignDaiJianpjsqsj").toString
        assignDaiJianpjwlbm = hit.getSourceAsMap.get("assignDaiJianpjwlbm").toString
        assignDaiJianpjwlmc = hit.getSourceAsMap.get("assignDaiJianpjwlmc").toString
        assignDaiJianpjwlsl = hit.getSourceAsMap.get("assignDaiJianpjwlsl").toString
        assignDaiJiandjquyunum = hit.getSourceAsMap.get("assignDaiJiandjquyunum").toString
        assignDaiJiandjxsgsnum = hit.getSourceAsMap.get("assignDaiJiandjxsgsnum").toString
        assignDaiJiandjwdnum = hit.getSourceAsMap.get("assignDaiJiandjwdnum").toString
        assignDaiJiandjwd = hit.getSourceAsMap.get("assignDaiJiandjwd").toString
        assignDaiJiandjwdmc = hit.getSourceAsMap.get("assignDaiJiandjwdmc").toString
        assignDaiJiandjsj = hit.getSourceAsMap.get("assignDaiJiandjsj").toString
        assignDaiJiansplb = hit.getSourceAsMap.get("assignDaiJiansplb").toString
        assignDaiJiancjdt = hit.getSourceAsMap.get("assignDaiJiancjdt").toString
        assignDaiJianthwlbm = hit.getSourceAsMap.get("assignDaiJianthwlbm").toString
        assignDaiJianthwlmc = hit.getSourceAsMap.get("assignDaiJianthwlmc").toString
        assignDaiJianthbmxsgsnum = hit.getSourceAsMap.get("assignDaiJianthbmxsgsnum").toString
        assignDaiJianthbmqynum = hit.getSourceAsMap.get("assignDaiJianthbmqynum").toString
        assignDaiJianthbmwdnum = hit.getSourceAsMap.get("assignDaiJianthbmwdnum").toString
        assignDaiJianpjxtflag = hit.getSourceAsMap.get("assignDaiJianpjxtflag").toString
        appointmentkssj = hit.getSourceAsMap.get("appointmentkssj").toString
        appointmentjssj = hit.getSourceAsMap.get("appointmentjssj").toString
        chaoshitime = hit.getSourceAsMap.get("chaoshitime").toString
        yqwangongtime = hit.getSourceAsMap.get("yqwangongtime").toString
        wangongfknr = hit.getSourceAsMap.get("wangongfknr").toString
        shijiwangongtime = hit.getSourceAsMap.get("shijiwangongtime").toString


      } catch {
        case e: Exception => logger.error("UpdateWxDaiJianKuanBiao查wx_export_data_wait_part_orders_v3表异常->" + e.getMessage)
      }

      out.collect(KuanBiaoDaiJianData(pgid, created_by, created_date, last_modified_by, last_modified_date, AUTH_STATE, azsl,
        beiz, bjustat, chaoshiqe, cjdt, cjren, cjrmc, cjwdno, cshi, cxyzm,
        dhhm, dizi, dqjdsj, email, fjhm, fwrybwgsj, gdhao, gpsdzxx, jindu, ldcs, pgguid, qqlymc, qqlyxh, qqlyzj,
        quhao, qwsmjssj, qystat, sfen, sffswx, spid, spmc, ssqy, stat, tsdengji, wcsj, weidu, wwsl, wxren, wxrenid,
        wxshul, wxwdmc, wxwdno, xian, xjwdmc, xjwdno, xjwdsj, xqxiaolei, xsdh, xsorsh, xswdmc, xswdno, xxlb, xxly,
        xxqd, xzhen, yddh, yddh2, yhgyhf, yhif, yhmc, yhqwsmsj, yhsx, yhyyczsj, yxji, zbby, zjczsj, zjczwd, zjczwdxtbh,
        zptype, zxhao, cshiid, sfenid, xianid, xxlbid, xxlyid, xxqdid, yhsxid, appointmentkssj,appointmentjssj,assignDaiJianid,
        assignDaiJianpjsqbh,assignDaiJianpjsqsj,assignDaiJianpjwlbm,assignDaiJianpjwlmc,assignDaiJianpjwlsl,assignDaiJiandjquyunum,
        assignDaiJiandjxsgsnum,assignDaiJiandjwdnum,assignDaiJiandjwd,assignDaiJiandjwdmc,assignDaiJiandjsj,assignDaiJiansplb,assignDaiJiancjdt,
        assignDaiJianthwlmc,assignDaiJianthbmxsgsnum,assignDaiJianthbmqynum,assignDaiJianthbmwdnum,assignDaiJianpjxtflag , yqwangongtime, chaoshitime,wangongfknr,shijiwangongtime,assignDaiJianthwlbm,
        value.first, value.second, value.third, value.fourth, value.fifth, value.sixth, value.seventh,"null","null","null","null","null","null","null"))
    }
    ESTransportPoolUtil.returnClient(client)
  }
}
