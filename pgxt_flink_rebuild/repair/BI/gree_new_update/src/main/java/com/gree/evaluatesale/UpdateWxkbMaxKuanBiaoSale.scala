package com.gree.evaluatesale

import java.util

import com.gree.model.{MaxKuanBiao, WangdianLevel}
import com.gree.util.{ESTransportPoolUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

class UpdateWxkbMaxKuanBiaoSale extends ProcessFunction[WangdianLevel,MaxKuanBiao] {
  override def processElement(value: WangdianLevel, ctx: ProcessFunction[WangdianLevel, MaxKuanBiao]#Context, out: Collector[MaxKuanBiao]): Unit = {
    //ES链接
    val client: TransportClient = ESTransportPoolUtil.getClient
    val logger: Logger = LoggerFactory.getLogger(UpdateWxkbMaxKuanBiaoSale.super.getClass)

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
    var xzhenid:String = "null"
    var wxcount:String = "null"
    var extjson1:String = "null"
    var extjson2:String = "null"
    var extjson3:String = "null"
    var extjson4:String = "null"
    var extjson5:String = "null"
    var yuYueBiaoArray : util.ArrayList[util.HashMap[String,String]] =null
    var fkmxBiaoArray : util.ArrayList[util.HashMap[String,String]] =null
    var xinXinXiBiaoArray : util.ArrayList[util.HashMap[String,String]] =null
    var daiJianBiaoArray : util.ArrayList[util.HashMap[String,String]] =null
    var fanKuiBiaoArray : util.ArrayList[util.HashMap[String,String]] =null
    var mingXiBiaoArray : util.ArrayList[util.HashMap[String,String]] =null
    var manYiBiaoArray : util.ArrayList[util.HashMap[String,String]] =null
    var appointmentkssj :String = "null"
    var appointmentjssj :String ="null"
    var yqwangongtime :String ="null"
    var fklb :String ="null"
    var fkjg :String ="null"
    var fksj :String ="null"
    var unReadXxx: String = "null"
    var baowangongtime:String = "null"
    var isOrNotDaiJian:String = "null"
    var vip:String = "null"

    val response: SearchResponse = client
      .prepareSearch("wxkb_max_big_kuanbiao_v1")
      .setTypes("_doc")
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("xjwdno", value.wdno))
        .must(QueryBuilders.termQuery("spid", value.splb))
        .must(QueryBuilders.rangeQuery("stat").lte(40)))
      .setSize(30000)
      .get()

    val hits: SearchHits = response.getHits
    logger.info("wxkb_max_big_kuanbiao_v1符合的数据有" + hits.totalHits + "条")

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
        xzhenid = hit.getSourceAsMap.get("xzhenid").toString
        wxcount = hit.getSourceAsMap.get("wxcount").toString
        extjson1 = hit.getSourceAsMap.get("extjson1").toString
        extjson2 = hit.getSourceAsMap.get("extjson2").toString
        extjson3 = hit.getSourceAsMap.get("extjson3").toString
        extjson4 = hit.getSourceAsMap.get("extjson4").toString
        extjson5 = hit.getSourceAsMap.get("extjson5").toString
        yuYueBiaoArray = hit.getSourceAsMap.get("tbl_assign_appointment").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        fkmxBiaoArray = hit.getSourceAsMap.get("tbl_assign_fkmx").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        xinXinXiBiaoArray = hit.getSourceAsMap.get("tbl_assign_xzyd").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        daiJianBiaoArray = hit.getSourceAsMap.get("tbl_assign_daijian").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        fanKuiBiaoArray = hit.getSourceAsMap.get("tbl_assign_feedback").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        mingXiBiaoArray = hit.getSourceAsMap.get("tbl_assign_mx").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        manYiBiaoArray = hit.getSourceAsMap.get("tbl_assign_satisfaction").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        appointmentkssj = hit.getSourceAsMap.get("appointmentkssj").toString
        appointmentjssj = hit.getSourceAsMap.get("appointmentjssj").toString
        yqwangongtime = hit.getSourceAsMap.get("yqwangongtime").toString
        fklb = hit.getSourceAsMap.get("fkmx_fklb").toString
        fkjg = hit.getSourceAsMap.get("fkmx_fkjg").toString
        fksj = hit.getSourceAsMap.get("fkmx_fksj").toString
        unReadXxx = hit.getSourceAsMap.get("unReadXxx").toString
        baowangongtime = hit.getSourceAsMap.get("baowangongtime").toString
        isOrNotDaiJian = hit.getSourceAsMap.get("isOrNotDaiJian").toString
        vip = hit.getSourceAsMap.get("vip").toString
      } catch {
        case e: Exception => logger.error("UpdateWxkbMaxKuanBiao查wxkb_max_big_kuanbiao_v1表异常->" + e.getMessage)
      }

      out.collect(MaxKuanBiao( pgid, created_by, created_date, last_modified_by, last_modified_date, AUTH_STATE, azsl,
        beiz, bjustat, chaoshiqe, cjdt, cjren, cjrmc, cjwdno, cshi,cshiid,cxyzm,
        dhhm, dizi, dqjdsj, email, fjhm, fwrybwgsj, gdhao, gpsdzxx, jindu, ldcs, pgguid, qqlymc, qqlyxh, qqlyzj,
        quhao, qwsmjssj, qystat, sfen, sfenid,sffswx, spid, spmc, ssqy, stat, tsdengji, wcsj, weidu, wwsl, wxren, wxrenid,
        wxshul, wxwdmc, wxwdno, xian,  xianid,xjwdmc, xjwdno, xjwdsj, xqxiaolei, xsdh, xsorsh, xswdmc, xswdno, xxlb, xxlbid, xxly,
        xxlyid, xxqd,xxqdid, xzhen, yddh, yddh2, yhgyhf, yhif, yhmc, yhqwsmsj, yhsx, yhyyczsj, yxji, zbby, zjczsj, zjczwd, zjczwdxtbh,
        zptype, zxhao, yhsxid,xzhenid,yuYueBiaoArray,fkmxBiaoArray,xinXinXiBiaoArray,daiJianBiaoArray,fanKuiBiaoArray,mingXiBiaoArray,manYiBiaoArray,
        "null", "null", "null", "null", "null", "null", "null",value.first,value.second,value.third,value.fourth,value.fifth,value.sixth,value.seventh,
        appointmentkssj,appointmentjssj,yqwangongtime,fklb,fkjg,fksj,unReadXxx,wxcount,extjson1,extjson2,extjson3,extjson4,extjson5,baowangongtime,isOrNotDaiJian,vip))
    }
    ESTransportPoolUtil.returnClient(client)
  }
}