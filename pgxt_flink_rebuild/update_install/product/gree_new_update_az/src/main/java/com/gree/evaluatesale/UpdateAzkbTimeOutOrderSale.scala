package com.gree.evaluatesale

import com.gree.model.{AzDataChaoShi, WangdianLevel}
import com.gree.util.EsConnection
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

//待改派
class UpdateAzkbTimeOutOrderSale extends ProcessFunction[WangdianLevel,AzDataChaoShi] {
  override def processElement(value: WangdianLevel, ctx: ProcessFunction[WangdianLevel, AzDataChaoShi]#Context, out: Collector[AzDataChaoShi]): Unit = {
    //ES链接
    val client: TransportClient = EsConnection.conn
    val logger: Logger = LoggerFactory.getLogger(UpdateAzkbTimeOutOrderSale.super.getClass)

    /*  gte：大于等于
    gt：大于
    lte：小于等于
    lt：小于*/

    var pgguid	: String= "null"		//	派工信息单主键,
    var  created_by	: String= "null"		//
    var created_date	: String= "null"		//
    var last_modified_by	: String= "null"		//
    var  last_modified_date	: String= "null"	//
    var  pgid	: String= "null"		//	派工序号,
    var yhmc	: String= "null"		//	姓名,
    var yddh	: String= "null"	//	移动电话,
    var yddh2	: String= "null"		//	移动电话2,
    var  quhao	: String= "null"		//	区号,
    var dhhm	: String= "null"	//	电话号码,
    var fjhm	: String= "null"		//	分机号,
    var email	: String= "null"		//	电子邮箱,
    var  sfen	: String= "null"		//	省份,
    var  cshi	: String= "null"		//	市,
    var xian	: String= "null"	//	县,
    var  xzhen	: String= "null"		//	街道办/乡镇,
    var dizi	: String= "null"		//	地址,
    var xxqd	: String= "null"		//	信息渠道,
    var  xxly	: String= "null"		//	信息来源,
    var xxlb	: String= "null"		//	信息类别,
    var beiz	: String= "null"	//	备注,
    var yhsx	: String= "null"		//	用户属性,
    var yxji	: String= "null"		//	优先级,
    var gdhao	: String= "null"		//	归档号,
    var spid	: String= "null"	//	商品类别序号,
    var  spmc	: String= "null"		//	品类名称,
    var  azren	: String= "null"	//	安装人,
    var azrenid	: String= "null"		//	安装人ID,
    var azwdxtbh	: String= "null"		//	安装网点系统编号,
    var azwdno	: String= "null"	//	安装网点,
    var  azwdmc	: String= "null"		//	安装网点名称,
    var   jspgwdno	: String= "null"		//	接收派工网点,
    var jspgwdmc	: String= "null"		//	接收派工网点名称,
    var  jspgwdxtbh	: String= "null"		//	接收派工网点系统编号,
    var jspgwdsj	: String= "null"		//	接收派工时间,
    var zxha	: String= "null"	//	座席号,
    var ssqy	: String= "null"	//	所属区域,
    var qqlyno	: String= "null"	//	来源系统,
    var qqlymc	: String= "null"	//	信息方式名称,
    var qqlyxh	: String= "null"	//	来源序号,
    var qqlyzj	: String= "null"	//	来源系统主信息主键,
    var  bjustat	: String= "null"	//	检测北交云扫描状态,
    var yhqwkssj	: String= "null"	//	用户期望上门开始时间,
    var  yhqwjssj	: String= "null"		//	用户期望上门结束时间,
    var  stat	: String= "null"	//	派工状态,
    var gpsdzxx	: String= "null"	//	GPS地址信息,
    var  cjren	: String= "null"	//	创建人,
    var  cjrmc	: String= "null"		//	创建人姓名,
    var cjdt	: String= "null"	//	创建时间,
    var cjwdno	: String= "null"	//	创建网点,
    var cjwdxtbh	: String= "null"		//	创建网点系统,
    var zjczren	: String= "null"	//	最近操作人,
    var zjczwd	: String= "null"	//	最近操作网点,
    var zjczwdxtbh	: String= "null"	//	系统网点编号,
    var zjczsj	: String= "null"		//	最近操作时间,
    var xslx	: String= "null"	//	销售类型,
    var lcid	: String= "null"	//	流程序号,
    var djlxno	: String= "null"	//	单据类型编号,
    var yyazsj	: String= "null"	//	预约安装时间,
    var sfwcps	: String= "null"		//	是否完成配送,
    var xsdh	: String= "null"	//	销售单号,
    var gcbh	: String= "null"	//	工程编号,
    var gcmc	: String= "null"	//	工程名称,
    var  azsl	: String= "null"	//	安装总数量,
    var wwsl	: String= "null"	//	未完成数量,
    var lxren	: String= "null"	//	联系人,
    var xsdwno	: String= "null"		//	销售单位编号,
    var xswdmc	: String= "null"	//	销售单位名称,
    var xsdwxtbh	: String= "null"	//	销售单位系统网点编号,
    var fphm	: String= "null"	//	发票号码,
    var gmsj	: String= "null"		//	购买日期,
    var kqbh	: String= "null"	//	跨区编号,
    var xsorsh	: String= "null"	//	数据录入属性,
    var dqjdsj	: String= "null"	//	当前节点时间,
    var syjd	: String= "null"//	上一节点,
    var dqjd	: String= "null"		//	当前节点,
    var jindu	: String= "null"	//	经度,
    var weidu	: String= "null"	//	纬度,
    var shsj	: String= "null"	//	送货时间,
    var sfygllc	: String= "null"	//	是否有关联流程,
    var xslxid	: String= "null"	//	销售类型id,
    var yhsxid	: String= "null"	//	用户属性id,
    var xxlbid	: String= "null"	//	信息类别id,
    var xxlyid	: String= "null"	//	信息来源id,
    var sfenid	: String= "null"	//	省份id,
    var cshiid	: String= "null"	//	市id,
    var xianid	: String= "null"	//	县id,
    var xzhenid	: String= "null"	//	乡镇id,
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
    var  cljggz : String= "null"//处理结果跟踪
    var appointmentkssj : String= "null"//用户预约时间 : 开始时间
    var appointmentjssj : String= "null"//用户预约时间：结束时间
    var yqwangongtime:String= "null" //用户要求完工时间
    var chaoshishichang : String= "null" //超时时长，完工时间
    var shijiwangongtime : String= "null"


    val response: SearchResponse = client
      .prepareSearch("azkb_timeout_orders_v3")
      .setTypes("_doc")
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("cjwdno", value.wdno))
        .must(QueryBuilders.termQuery("spid", value.splb))
        .must(QueryBuilders.rangeQuery("dqjd").lte(1304)))
      .setSize(30000)
      .get()

    val hits: SearchHits = response.getHits
    logger.info("azkb_timeout_orders_v3符合的数据有" + hits.totalHits + "条")

    for (hit <- hits) {

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
        cljggz = hit.getSourceAsMap.get("cljggz").toString
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
        appointmentkssj = hit.getSourceAsMap.get("appointmentkssj").toString
        appointmentjssj = hit.getSourceAsMap.get("appointmentjssj").toString
        yqwangongtime= hit.getSourceAsMap.get("yqwangongtime").toString
        chaoshishichang= hit.getSourceAsMap.get("chaoshishichang").toString
        shijiwangongtime = hit.getSourceAsMap.get("shijiwangongtime").toString



      } catch {
        case e: Exception => logger.error("UpdateAzkbTimeOutOrder查azkb_timeout_orders_v3表异常->" + e.getMessage)
      }

      out.collect(AzDataChaoShi(
        pgguid,created_by,created_date,last_modified_by,last_modified_date,pgid,yhmc,yddh
        ,yddh2,quhao,dhhm,fjhm,email,sfen,cshi,xian,xzhen,dizi,xxqd,xxly,xxlb,beiz,yhsx
        ,yxji,gdhao,spid,spmc,azren,azrenid,azwdxtbh,azwdno,azwdmc,jspgwdno,jspgwdmc,jspgwdxtbh,jspgwdsj,zxha,ssqy,qqlyno,qqlymc
        ,qqlyxh,qqlyzj,bjustat,yhqwkssj,yhqwjssj,stat,gpsdzxx,cjren,cjrmc,cjdt,cjwdno,cjwdxtbh,zjczren,zjczwd,zjczwdxtbh,zjczsj,xslx
        ,lcid,djlxno,yyazsj,sfwcps,xsdh,gcbh,gcmc,azsl,wwsl,lxren,xsdwno,xswdmc,xsdwxtbh,fphm,gmsj,kqbh,xsorsh,dqjdsj,syjd,dqjd
        ,jindu,weidu,shsj,sfygllc,xslxid,yhsxid,xxlbid,xxlyid,sfenid,cshiid,xianid,xzhenid,wcsj,retailsign,servicewdmc,shopname,
        orderphone,extendfiled1,extendfiled2,extendfiled3,extendfiled4,servicewdno,shopno,extendfiled5,
        cljggz, appointmentkssj,appointmentjssj,yqwangongtime,chaoshishichang,shijiwangongtime,"null","null","null","null","null","null","null",value.first,
        value.second, value.third, value.fourth, value.fifth, value.sixth, value.seventh))

    }
  }
}
