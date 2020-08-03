package com.gree.evaluatesale

import com.gree.model.{MaxKuanBiaoData, WangdianLevel}
import com.gree.util.EsConnection
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

//
class UpdateAzkuanbiaoAllOrderSale extends ProcessFunction[WangdianLevel,MaxKuanBiaoData] {
  override def processElement(value: WangdianLevel, ctx: ProcessFunction[WangdianLevel, MaxKuanBiaoData]#Context, out: Collector[MaxKuanBiaoData]): Unit = {
    //ES链接
    val client: TransportClient = EsConnection.conn
    val logger: Logger = LoggerFactory.getLogger(UpdateAzkuanbiaoAllOrderSale.super.getClass)

    /*  gte：大于等于
    gt：大于
    lte：小于等于
    lt：小于*/
    //安装主表
    //安装主表
    var	   pgguid: String= "null"//'派工信息单主键'
    var	   created_by: String= "null"
    var	   created_date: String= "null"
    var	   last_modified_by: String= "null"
    var	   last_modified_date: String= "null"
    var	   pgid: String= "null" //'派工序号'
    var	   yhmc: String= "null" //'姓名'
    var	   yddh: String= "null" //'移动电话'
    var	   yddh2: String= "null" //'移动电话2'
    var	   quhao: String= "null" //'区号'
    var	   dhhm: String= "null" //'电话号码'
    var	   fjhm: String= "null" //'分机号'
    var	   email: String= "null" //'电子邮箱'
    var	   sfen: String= "null" //'省份'
    var	   cshi: String= "null" //'市'
    var	   xian: String= "null" //'县'
    var	   xzhen: String= "null" //'街道办/乡镇'
    var	   dizi: String= "null" //'地址'
    var	   xxqd: String= "null" //'信息渠道'
    var	   xxly: String= "null" //'信息来源'
    var	   xxlb: String= "null" //'信息类别'
    var	   beiz: String= "null" //'备注'
    var	   yhsx: String= "null" //'用户属性'
    var	   yxji: String= "null" //'优先级'
    var	   gdhao: String= "null" //'归档号'
    var	   spid: String= "null" //'商品类别序号'
    var	   spmc: String= "null" //'品类名称'
    var	   azren: String= "null" //'安装人'
    var	   azrenid: String= "null" //'安装人ID'
    var	   azwdxtbh: String= "null" //'安装网点系统编号'
    var	   azwdno: String= "null" //'安装网点'
    var	   azwdmc: String= "null" //'安装网点名称'
    var	   jspgwdno: String= "null" //'接收派工网点'
    var	   jspgwdmc: String= "null" //'接收派工网点名称'
    var	   jspgwdxtbh: String= "null" //'接收派工网点系统编号'
    var	   jspgwdsj: String= "null" //'接收派工时间'
    var	   zxha: String= "null" //'座席号'
    var	   ssqy: String= "null" //'所属区域'
    var	   qqlyno: String= "null" //'来源系统'
    var	   qqlymc: String= "null" //'信息方式名称'
    var	   qqlyxh: String= "null" //'来源序号'
    var	   qqlyzj: String= "null" //'来源系统主信息主键'
    var	   bjustat: String= "null" //'检测北交云扫描状态'
    var	   yhqwkssj: String= "null" //'用户期望上门开始时间'
    var	   yhqwjssj: String= "null" //'用户期望上门结束时间'
    var	   stat: String= "null" //'派工状态'
    var	   gpsdzxx: String= "null" //'GPS地址信息'
    var	   cjren: String= "null" //'创建人'
    var	   cjrmc: String= "null" //'创建人姓名'
    var	   cjdt: String= "null" //'创建时间'
    var	   cjwdno: String= "null" //'创建网点'
    var	   cjwdxtbh: String= "null" //'创建网点系统'
    var	   zjczren: String= "null" //'最近操作人'
    var	   zjczwd: String= "null" //'最近操作网点'
    var	   zjczwdxtbh: String= "null" //'系统网点编号'
    var	   zjczsj: String= "null" //'最近操作时间'
    var	   xslx: String= "null" //'销售类型'
    var	   lcid: String= "null" //'流程序号'
    var	   djlxno: String= "null" //'单据类型编号'
    var	   yyazsj: String= "null" //'预约安装时间'
    var	   sfwcps: String= "null" //'是否完成配送'
    var	   xsdh: String= "null" //'销售单号'
    var	   gcbh: String= "null" //'工程编号'
    var	   gcmc: String= "null" //'工程名称'
    var	   azsl: String= "null" //'安装总数量'
    var	   wwsl: String= "null" //'未完成数量'
    var	   lxren: String= "null" //'联系人'
    var	   xsdwno: String= "null" //'销售单位编号'
    var	   xswdmc: String= "null" //'销售单位名称'
    var	   xsdwxtbh: String= "null" //'销售单位系统网点编号'
    var	   fphm: String= "null" //'发票号码'
    var	   gmsj: String= "null" //'购买日期'
    var	   kqbh: String= "null" //'跨区编号'
    var	   xsorsh: String= "null" //'数据录入属性'
    var	   dqjdsj: String= "null" //'当前节点时间'
    var	   syjd: String= "null" //'上一节点'
    var	   dqjd: String= "null" //'当前节点'
    var	   jindu: String= "null" //'经度'
    var	   weidu: String= "null" //'纬度'
    var	   shsj: String= "null" //'送货时间'
    var	   sfygllc: String= "null" //'是否有关联流程'
    var	   xslxid: String= "null" //'销售类型id'
    var	   yhsxid: String= "null" //'用户属性id'
    var	   xxlbid: String= "null" //'信息类别id'
    var	   xxlyid: String= "null" //'信息来源id'
    var	   sfenid: String= "null" //'省份id'
    var	   cshiid: String= "null" //'市id'
    var	   xianid: String= "null" //'县id'
    var	   xzhenid: String= "null" //'乡镇id'
    /*   tblAzAssignMxPgmxid: String= "null" //'明细序号（主键）'
			var	   tblAzAssignMxMxspid: String= "null" //'产品大类序号'
			var	   tblAzAssignMxMxspmc: String= "null" //'产品大类名称'
			var	   tblAzAssignMxXlid: String= "null" //'小类序号'
			var	   tblAzAssignMxXlmc: String= "null" //'小类名称'
			var	   tblAzAssignMxXiid: String= "null" //'系列序号'
			var	   tblAzAssignMxXimc: String= "null" //'系列名称'
			var	   tblAzAssignMxJxmc: String= "null" //'机型名称'
			var	   tblAzAssignMxJxno: String= "null" //'机型编号'
			var	   tblAzAssignMxMxczren: String= "null" //'操作人'
			var	   tblAzAssignMxMxczsj: String= "null" //'操作时间'
			var	   tblAzAssignMxCzwd: String= "null" //'操作网点'
			var	   tblAzAssignMxNjtm: String= "null" //'内机条码'
			var	   tblAzAssignMxWjtm: String= "null" //'外机条码'
			var	   tblAzAssignMxMxbeiz: String= "null" //'明细备注'
			var	   tblAzAssignMxShul: String= "null" //'数量'
			var	   tblAzAssignMxCmxjdt: String= "null" //'创建时间'
			var	   tblAzAssignMxJiage: String= "null" //'价格'
			var	   tblAzAssignMxDanw: String= "null" //'单位'
			var	   tblAzAssignMxWldm: String= "null" //'物料代码'
			var	   tblAzAssignMxNjtm2: String= "null" //'内机条码2'
			var	   tblAzAssignMxWjsl: String= "null" //'外机数量'
			var	   tblAzAssignMxNjsl: String= "null" //'内机数量'
			var	   tblAzAssignMxMxwwsl: String= "null" //'未完成数量'*/
    var	   tblAzAssignFkmxFkid: String= "null" //'主键'
    var	   tblAzAssignFkmxFklb: String= "null" //'反馈类别'
    var	   tblAzAssignFkmxFkjg: String= "null" //'反馈结果'
    var	   tblAzAssignFkmxFknr: String= "null" //'反馈内容'
    var	   tblAzAssignFkmxFkren: String= "null" //'反馈人'
    var	   tblAzAssignFkmxFkrenmc: String= "null" //'反馈人姓名'
    var	   tblAzAssignFkmxFksj: String= "null" //'反馈时间'
    var	   tblAzAssignFkmxXtwdbh: String= "null" //'系统网点编号'
    var	   tblAzAssignFkmxWdno: String= "null" //'反馈网点'
    var	   tblAzAssignFkmxWdmc: String= "null" //'反馈网点名称'
    var	   tblAzAssignFkmxFkmxcjdt: String= "null" //'创建时间'
    var	   tblAzAssignAppointmentId: String= "null" //'主键'
    var	   tblAzAssignAppointmentKssj: String= "null" //'用户预约时间:开始时间'
    var	   tblAzAssignAppointmentJssj: String= "null" //'用户预约时间：结束时间'
    var	   tblAzAssignAppointmentCzren: String= "null" //'操作人'
    var	   tblAzAssignAppointmentCzsj: String= "null" //'操作时间'
    var	   tblAzAssignAppointmentLeix: String= "null" //'类型'
    var	   tblAzAssignAppointmentReason: String= "null" //'延误类型'
    var	   tblAzAssignAppointmentBeiz: String= "null" //备注
    var	   yqwangongtime: String= "null" // 要求完工时间(有预约时间:取最新开始时间+24h & 无预约时间:取安装主表创建时间+24小时)
    var	   shoucipaigongtime:String= "null"
    var	   shijiwangongtime:String= "null"

    val response: SearchResponse = client
      .prepareSearch("az_export_data_all_orders_v1")
      .setTypes("_doc")
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("cjwdno", value.wdno))
        .must(QueryBuilders.termQuery("spid", value.splb))
        .must(QueryBuilders.rangeQuery("dqjd").lte(1304)))
      .setSize(30000)
      .get()

    val hits: SearchHits = response.getHits
    logger.info("az_export_data_all_orders_v3符合的数据有" + hits.totalHits + "条")

    for (hit <- hits) {

      try {
        //安装主表
        pgguid = hit.getSourceAsMap.get("pgguid").toString//'派工信息单主键'
        created_by=hit.getSourceAsMap.get("created_by").toString
        created_date=hit.getSourceAsMap.get("created_date").toString
        last_modified_by=hit.getSourceAsMap.get("last_modified_by").toString
        last_modified_date=hit.getSourceAsMap.get("last_modified_date").toString
        pgid = hit.getSourceAsMap.get("pgid").toString//'派工序号'
        yhmc = hit.getSourceAsMap.get("yhmc").toString//'姓名'
        yddh = hit.getSourceAsMap.get("yddh").toString//'移动电话'
        yddh2 = hit.getSourceAsMap.get("yddh2").toString//'移动电话2'
        quhao = hit.getSourceAsMap.get("quhao").toString//'区号'
        dhhm = hit.getSourceAsMap.get("dhhm").toString//'电话号码'
        fjhm = hit.getSourceAsMap.get("fjhm").toString//'分机号'
        email = hit.getSourceAsMap.get("email").toString//'电子邮箱'
        sfen = hit.getSourceAsMap.get("sfen").toString//'省份'
        cshi = hit.getSourceAsMap.get("cshi").toString//'市'
        xian = hit.getSourceAsMap.get("xian").toString//'县'
        xzhen = hit.getSourceAsMap.get("xzhen").toString//'街道办/乡镇'
        dizi = hit.getSourceAsMap.get("dizi").toString//'地址'
        xxqd = hit.getSourceAsMap.get("xxqd").toString//'信息渠道'
        xxly = hit.getSourceAsMap.get("xxly").toString//'信息来源'
        xxlb = hit.getSourceAsMap.get("xxlb").toString//'信息类别'
        beiz = hit.getSourceAsMap.get("beiz").toString//'备注'
        yhsx = hit.getSourceAsMap.get("yhsx").toString//'用户属性'
        yxji = hit.getSourceAsMap.get("pgid").toString//'优先级'
        gdhao = hit.getSourceAsMap.get("yxji").toString//'归档号'
        spid = hit.getSourceAsMap.get("spid").toString//'商品类别序号'
        spmc = hit.getSourceAsMap.get("spmc").toString//'品类名称'
        azren = hit.getSourceAsMap.get("azren").toString//'安装人'
        azrenid = hit.getSourceAsMap.get("azrenid").toString//'安装人ID'
        azwdxtbh = hit.getSourceAsMap.get("azwdxtbh").toString//'安装网点系统编号'
        azwdno = hit.getSourceAsMap.get("azwdno").toString//'安装网点'
        azwdmc = hit.getSourceAsMap.get("azwdmc").toString//'安装网点名称'
        jspgwdno = hit.getSourceAsMap.get("jspgwdno").toString//'接收派工网点'
        jspgwdmc = hit.getSourceAsMap.get("jspgwdmc").toString//'接收派工网点名称'
        jspgwdxtbh = hit.getSourceAsMap.get("jspgwdxtbh").toString//'接收派工网点系统编号'
        jspgwdsj = hit.getSourceAsMap.get("jspgwdsj").toString//'接收派工时间'
        zxha = hit.getSourceAsMap.get("zxha").toString//'座席号'
        ssqy = hit.getSourceAsMap.get("ssqy").toString//'所属区域'
        qqlyno = hit.getSourceAsMap.get("qqlyno").toString//'来源系统'
        qqlymc = hit.getSourceAsMap.get("qqlymc").toString//'信息方式名称'
        qqlyxh = hit.getSourceAsMap.get("qqlyxh").toString//'来源序号'
        qqlyzj = hit.getSourceAsMap.get("qqlyzj").toString//'来源系统主信息主键'
        bjustat = hit.getSourceAsMap.get("bjustat").toString//'检测北交云扫描状态'
        yhqwkssj = hit.getSourceAsMap.get("yhqwkssj").toString//'用户期望上门开始时间'
        yhqwjssj = hit.getSourceAsMap.get("yhqwjssj").toString//'用户期望上门结束时间'
        stat = hit.getSourceAsMap.get("stat").toString//'派工状态'
        gpsdzxx = hit.getSourceAsMap.get("gpsdzxx").toString//'GPS地址信息'
        cjren = hit.getSourceAsMap.get("cjren").toString//'创建人'
        cjrmc = hit.getSourceAsMap.get("cjrmc").toString//'创建人姓名'
        cjdt = hit.getSourceAsMap.get("cjdt").toString//'创建时间'
        cjwdno = hit.getSourceAsMap.get("cjwdno").toString//'创建网点'
        cjwdxtbh = hit.getSourceAsMap.get("cjwdxtbh").toString//'创建网点系统'
        zjczren = hit.getSourceAsMap.get("zjczren").toString//'最近操作人'
        zjczwd = hit.getSourceAsMap.get("zjczwd").toString//'最近操作网点'
        zjczwdxtbh = hit.getSourceAsMap.get("zjczwdxtbh").toString//'系统网点编号'
        zjczsj = hit.getSourceAsMap.get("zjczsj").toString//'最近操作时间'
        xslx = hit.getSourceAsMap.get("xslx").toString//'销售类型'
        lcid = hit.getSourceAsMap.get("lcid").toString//'流程序号'
        djlxno = hit.getSourceAsMap.get("djlxno").toString//'单据类型编号'
        yyazsj = hit.getSourceAsMap.get("yyazsj").toString//'预约安装时间'
        sfwcps = hit.getSourceAsMap.get("sfwcps").toString//'是否完成配送'
        xsdh = hit.getSourceAsMap.get("xsdh").toString//'销售单号'
        gcbh = hit.getSourceAsMap.get("gcbh").toString//'工程编号'
        gcmc = hit.getSourceAsMap.get("gcmc").toString//'工程名称'
        azsl = hit.getSourceAsMap.get("azsl").toString//'安装总数量'
        wwsl = hit.getSourceAsMap.get("wwsl").toString//'未完成数量'
        lxren = hit.getSourceAsMap.get("lxren").toString//'联系人'
        xsdwno = hit.getSourceAsMap.get("xsdwno").toString//'销售单位编号'
        xswdmc = hit.getSourceAsMap.get("xswdmc").toString//'销售单位名称'
        xsdwxtbh = hit.getSourceAsMap.get("xsdwxtbh").toString//'销售单位系统网点编号'
        fphm = hit.getSourceAsMap.get("fphm").toString//'发票号码'
        gmsj = hit.getSourceAsMap.get("gmsj").toString//'购买日期'
        kqbh = hit.getSourceAsMap.get("kqbh").toString//'跨区编号'
        xsorsh = hit.getSourceAsMap.get("xsorsh").toString//'数据录入属性'
        dqjdsj = hit.getSourceAsMap.get("dqjdsj").toString//'当前节点时间'
        syjd = hit.getSourceAsMap.get("syjd").toString//'上一节点'
        dqjd = hit.getSourceAsMap.get("dqjd").toString//'当前节点'
        jindu = hit.getSourceAsMap.get("jindu").toString//'经度'
        weidu = hit.getSourceAsMap.get("weidu").toString//'纬度'
        shsj = hit.getSourceAsMap.get("shsj").toString//'送货时间'
        sfygllc = hit.getSourceAsMap.get("sfygllc").toString//'是否有关联流程'
        xslxid = hit.getSourceAsMap.get("xslxid").toString//'销售类型id'
        yhsxid = hit.getSourceAsMap.get("yhsxid").toString//'用户属性id'
        xxlbid = hit.getSourceAsMap.get("xxlbid").toString//'信息类别id'
        xxlyid = hit.getSourceAsMap.get("xxlyid").toString//'信息来源id'
        sfenid = hit.getSourceAsMap.get("sfenid").toString//'省份id'
        cshiid = hit.getSourceAsMap.get("cshiid").toString//'市id'
        xianid = hit.getSourceAsMap.get("xianid").toString//'县id'
        xzhenid = hit.getSourceAsMap.get("xzhenid").toString//'乡镇id'
        tblAzAssignFkmxFkid = hit.getSourceAsMap.get("tblAzAssignFkmxFkid").toString//'主键'
        tblAzAssignFkmxFklb = hit.getSourceAsMap.get("tblAzAssignFkmxFklb").toString//'反馈类别'
        tblAzAssignFkmxFkjg = hit.getSourceAsMap.get("tblAzAssignFkmxFkjg").toString//'反馈结果'
        tblAzAssignFkmxFknr = hit.getSourceAsMap.get("tblAzAssignFkmxFknr").toString//'反馈内容'
        tblAzAssignFkmxFkren = hit.getSourceAsMap.get("tblAzAssignFkmxFkren").toString//'反馈人'
        tblAzAssignFkmxFkrenmc = hit.getSourceAsMap.get("tblAzAssignFkmxFkrenmc").toString//'反馈人姓名'
        tblAzAssignFkmxFksj = hit.getSourceAsMap.get("tblAzAssignFkmxFksj").toString//'反馈时间'
        tblAzAssignFkmxXtwdbh = hit.getSourceAsMap.get("tblAzAssignFkmxXtwdbh").toString//'系统网点编号'
        tblAzAssignFkmxWdno = hit.getSourceAsMap.get("tblAzAssignFkmxWdno").toString//'反馈网点'
        tblAzAssignFkmxWdmc = hit.getSourceAsMap.get("tblAzAssignFkmxWdmc").toString//'反馈网点名称'
        tblAzAssignFkmxFkmxcjdt = hit.getSourceAsMap.get("tblAzAssignFkmxFkmxcjdt").toString//'创建时间'
        tblAzAssignAppointmentId = hit.getSourceAsMap.get("tblAzAssignAppointmentId").toString//'主键'
        tblAzAssignAppointmentKssj = hit.getSourceAsMap.get("tblAzAssignAppointmentKssj").toString//'用户预约时间:开始时间'
        tblAzAssignAppointmentJssj = hit.getSourceAsMap.get("tblAzAssignAppointmentJssj").toString//'用户预约时间：结束时间'
        tblAzAssignAppointmentCzren = hit.getSourceAsMap.get("tblAzAssignAppointmentCzren").toString//'操作人'
        tblAzAssignAppointmentCzsj = hit.getSourceAsMap.get("tblAzAssignAppointmentCzsj").toString//'操作时间'
        tblAzAssignAppointmentLeix = hit.getSourceAsMap.get("tblAzAssignAppointmentLeix").toString//'类型'
        tblAzAssignAppointmentReason = hit.getSourceAsMap.get("tblAzAssignAppointmentReason").toString//'延误类型'
        tblAzAssignAppointmentBeiz = hit.getSourceAsMap.get("tblAzAssignAppointmentBeiz").toString//备注
        yqwangongtime = hit.getSourceAsMap.get("yqwangongtime").toString// 要求完工时间(有预约时间:取最新开始时间+24h & 无预约时间:取安装主表创建时间+24小时)
        shoucipaigongtime= hit.getSourceAsMap.get("shoucipaigongtime").toString
        shijiwangongtime= hit.getSourceAsMap.get("shijiwangongtime").toString

      } catch {
      case e: Exception => logger.error("UpdateAzkbTimeOutOrder查az_export_data_all_orders_v1表异常->" + e.getMessage)
    }

      out.collect( MaxKuanBiaoData(pgguid,created_by,created_date,last_modified_by,last_modified_date,pgid,yhmc,yddh,yddh2,quhao,dhhm,fjhm,email,
        sfen,cshi,xian,xzhen,dizi,xxqd,xxly,xxlb,beiz,yhsx,yxji,gdhao,spid,spmc,azren,azrenid,azwdxtbh,azwdno,azwdmc,jspgwdno,jspgwdmc,jspgwdxtbh,
        jspgwdsj,zxha,ssqy,qqlyno,qqlymc,qqlyxh,qqlyzj,bjustat,yhqwkssj,yhqwjssj,stat,gpsdzxx,cjren,cjrmc,cjdt,cjwdno,cjwdxtbh,zjczren,zjczwd,zjczwdxtbh,
        zjczsj,xslx,lcid,djlxno,yyazsj,sfwcps,xsdh,gcbh,gcmc,azsl,wwsl,lxren,xsdwno,xswdmc,xsdwxtbh,fphm,gmsj,kqbh,xsorsh,dqjdsj,syjd,dqjd,jindu,weidu,
        shsj,sfygllc,xslxid,yhsxid,xxlbid,xxlyid,sfenid,cshiid,xianid,xzhenid,tblAzAssignFkmxFkid,tblAzAssignFkmxFklb,tblAzAssignFkmxFkjg,tblAzAssignFkmxFknr,tblAzAssignFkmxFkren,tblAzAssignFkmxFkrenmc,tblAzAssignFkmxFksj,
        tblAzAssignFkmxXtwdbh,tblAzAssignFkmxWdno,tblAzAssignFkmxWdmc,tblAzAssignFkmxFkmxcjdt,tblAzAssignAppointmentId,tblAzAssignAppointmentKssj,
        tblAzAssignAppointmentJssj,tblAzAssignAppointmentCzren,tblAzAssignAppointmentCzsj,tblAzAssignAppointmentLeix,tblAzAssignAppointmentReason,
        tblAzAssignAppointmentBeiz,yqwangongtime,shoucipaigongtime,shijiwangongtime,"null","null","null","null","null","null","null",value.first,
        value.second, value.third, value.fourth, value.fifth, value.sixth, value.seventh))

    }
    }

}
