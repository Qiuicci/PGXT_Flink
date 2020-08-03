package com.gree.func

import com.gree.model.KuanBiaoToTidb
import com.gree.util.{JDBCUtil, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

class KuanBiaoToMySqlSink  extends  RichSinkFunction[KuanBiaoToTidb]{
  val logger: Logger = LoggerFactory.getLogger(KuanBiaoToMySqlSink.super.getClass)

  override def invoke(value: KuanBiaoToTidb, context: SinkFunction.Context[_]): Unit = {
    this.synchronized{
      val util = new NumberFormatUtil
      logger.info("数据开始插入到TIDB中")

      try{
        logger.info("要插入的pgid为->"+value.wxgd_pgid)
        JDBCUtil.executeUpdate("replace into service_order values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
          Array(
            util.panduanDate(value.wxgd_cjdt),
            util.panduanNull(value.wxgd_beiz),
            util.panduanNull(value.wxgd_zbby),
            util.panduanNull(value.wxgd_xxlb),
            util.panduanNull(value.wxgd_xxqd),
            util.panduanNull(value.wxgd_xxly),
            util.panduanNull(value.wxgd_wxwdno),
            util.panduanNull(value.wxgd_wxwdmc),
            util.panduanNull(value.wxgd_pgid),
            util.panduanNull(value.wxgd_yhsx) ,
            util.panduanNull(value.wxgd_quhao) ,
            util.panduanNull(value.wxgd_xian) ,
            util.panduanNull(value.wxgd_stat) ,
            util.panduanNull(value.wxgd_cjren) ,
            util.panduanDate(value.wxgd_xjwdsj) ,
            util.panduanNull(value.wxgd_cjwdno) ,
            util.panduanNull(value.wxgd_xjwdno) ,
            util.panduanNull(value.wxgd_ssqy) ,
            util.panduanNull(value.wxgd_yhmc) ,
            util.panduanNull(value.wxgd_yddh) ,
            util.panduanNull(value.wxgd_sfen) ,
            util.panduanNull(value.wxgd_cshi),
            util.panduanNull(value.wxgd_spid) ,
            util.panduanNull(value.wxgd_spmc) ,
            util.panduanNull(value.wxgd_wxren) ,
            util.panduanNull(value.wxgd_wxrenid) ,
            util.panduanDate(value.wxgd_yhqwsmsj) ,
            util.panduanDate(value.wxgd_qwsmjssj) ,
            value.wxgdyd_zxxs ,
            util.panduanNull(value.wxgdmx_spmc) ,
            util.panduanNull(value.wxgdmx_xlmc) ,
            util.panduanNull(value.wxgdmx_gmsj) ,
            util.panduanNull(value.wxgdmx_jxmc) ,
            util.panduanNull(value.wxgdmx_gzxx) ,
            util.panduanNull(value.wxgdmyd_pjnr) ,
            util.panduanDate(value.wxgdyy_czsj) ,
            util.panduanDate(value.wxgdyy_kssj) ,
            util.panduanDate(value.wxgdyy_jssj) ,
            util.panduanNull(value.wxgdyy_yysj) ,
            util.panduanNull(value.bi_yqwgsj) ,
            util.panduanNull(value.wxgdfkmx_wg_fknr) ,
            util.panduanDate(value.wxgdfkmx_wg_fksj) ,
            util.panduanNull(value.wxgdfkmx_bh_fklb) ,
            util.panduanNull(value.wxgdfkmx_bh_fknr),
            util.panduanDate(value.wxgdfkmx_bh_fksj),
            util.panduanNull(value.wxgdfkmx_bh_fkwdmc) ,
            value.wxgdfkmx_dj
          )
        )

      }catch {
        case e:Exception => logger.error("插入tidb错误"+e.getMessage)
      }
    }
  }
}
