package com.gree.evaluate

import java.util

import com.gree.model.WangdianLevel
import com.gree.util.{ESTransportPoolUtil, SplitUtil}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.slf4j.{Logger, LoggerFactory}

class PanDuanLevelEqualsFunction(wdLevelData : OutputTag[WangdianLevel]) extends ProcessFunction[ObjectNode,WangdianLevel]{
  val logger: Logger = LoggerFactory.getLogger(PanDuanLevelEqualsFunction.super.getClass)

  override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, WangdianLevel]#Context, out: Collector[WangdianLevel]): Unit = {
    //ES链接
    val client: TransportClient = ESTransportPoolUtil.getClient
    //data中可能有多个对象，遍历取出
    val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()


      while(data.hasNext) {
        val wangdianbiao : JsonNode = data.next()
        if("售后".equals(wangdianbiao.get("fwlb").asText()) && "2".equals(wangdianbiao.get("stat").asText())) {

        //查询数据权限
        //从quanxianlevel_wangdianlevel_v1 ES中查出对应字段
        val quanXianLevel: SearchResponse = client
          .prepareSearch("quanxianlevel_wangdianlevel_v1")
          .setTypes("_doc")
          .setQuery(QueryBuilders.termQuery("id",wangdianbiao.get("id").asText()))
          .get()

        var level1:String = "null"
        var level2:String = "null"
        var level3:String = "null"
        var level4:String = "null"
        var level5:String = "null"
        var level6:String = "null"
        var level7:String = "null"
        var compar1:String = "null"
        var compar2:String = "null"
        var compar3:String = "null"
        var compar4:String = "null"
        var compar5:String = "null"
        var compar6:String = "null"
        var compar7:String = "null"

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
            case e:Exception => logger.error("安装PanDuanLevelEqualsFunction查询quanxianlevel_wangdianlevel_v1 ES表异常")
          }
        }

        val cxqyfwbh: String = wangdianbiao.get("cxqyfwbh").asText()
        val length: Int = (cxqyfwbh.length/8)
        val result: util.ArrayList[String] = SplitUtil.SplitFunctionFor8(cxqyfwbh)

        for (i <- 0 until length){
          if(i==0){
            compar1 = result.get(0)
            logger.info("compar1为"+compar1)
          }
          if(i==1){
            compar2 = result.get(1)
            logger.info("compar2为"+compar2)
          }
          if(i==2){
            compar3 = result.get(2)
            logger.info("compar3为"+compar3)
          }
          if(i==3){
            compar4 = result.get(3)
            logger.info("compar4为"+compar4)
          }
          if(i==4){
            compar5 = result.get(4)
            logger.info("compar5为"+compar5)
          }
          if(i==5){
            compar6 = result.get(5)
            logger.info("compar6为"+compar6)
          }
          if(i==6){
            compar7 = result.get(6)
            logger.info("compar7为"+compar7)
          }
        }

          //判断两者层级是否完全相同
        if (level1.equals(compar1)&&level2.equals(compar2)&&level3.equals(compar3)&&level4.equals(compar4)&&level5.equals(compar5)&&level6.equals(compar6)&&level7.equals(compar7)){
          logger.info("安装售后层级关系相同不做处理")
        }else{

          try {
            out.collect(WangdianLevel(
              wangdianbiao.get("id").asText(),
              wangdianbiao.get("created_by").asText(),
              wangdianbiao.get("created_date").asText(),
              wangdianbiao.get("last_modified_by").asText(),
              wangdianbiao.get("last_modified_date").asText(),
              wangdianbiao.get("xhid").asText(),
              wangdianbiao.get("xtwdbh").asText(),
              wangdianbiao.get("wdno").asText(),
              wangdianbiao.get("splb").asText(),
              wangdianbiao.get("scqy").asText(),
              wangdianbiao.get("fwlb").asText(),
              wangdianbiao.get("sjwdno").asText(),
              wangdianbiao.get("sjwdmc").asText(),
              wangdianbiao.get("sjwdxtbh").asText(),
              wangdianbiao.get("cxqyfw").asText(),
              wangdianbiao.get("stat").asText(),
              wangdianbiao.get("czren").asText(),
              wangdianbiao.get("czrmc").asText(),
              wangdianbiao.get("czsj").asText(),
              wangdianbiao.get("zhczsj").asText(),
              wangdianbiao.get("cxqyfwbh").asText(),
              wangdianbiao.get("sfxyzq").asText(),
              wangdianbiao.get("sfxyzqbak").asText(),
              wangdianbiao.get("seno").asText(),
              wangdianbiao.get("leix").asText(),
              wangdianbiao.get("wdmc").asText(),
              wangdianbiao.get("spmc").asText(),
              wangdianbiao.get("scqymc").asText(),
              compar1,
              compar2,
              compar3,
              compar4,
              compar5,
              compar6,
              compar7
            ))

          } catch {
            case e: Exception => logger.error("安装售后PanDuanLevelEqualsFunction抓取数据异常->" + e.getMessage)
          }
        }
        }


        //销售
        if("销售".equals(wangdianbiao.get("fwlb").asText()) && "2".equals(wangdianbiao.get("stat").asText())) {

          //查询数据权限
          //从quanxianlevel_wangdianlevel_v1 ES中查出对应字段
          val quanXianLevel: SearchResponse = client
            .prepareSearch("quanxianlevel_wangdianlevel_v1")
            .setTypes("_doc")
            .setQuery(QueryBuilders.termQuery("id",wangdianbiao.get("id").asText()))
            .get()

          var level8:String = "null"
          var level9:String = "null"
          var level10:String = "null"
          var level11:String = "null"
          var level12:String = "null"
          var level13:String = "null"
          var level14:String = "null"
          var compar8:String = "null"
          var compar9:String = "null"
          var compar10:String = "null"
          var compar11:String = "null"
          var compar12:String = "null"
          var compar13:String = "null"
          var compar14:String = "null"

          if(quanXianLevel.getHits.iterator().hasNext){
            val hit: SearchHit = quanXianLevel.getHits.iterator().next()
            try {
              level8 = hit.getSourceAsMap.get("first").toString
              level9 = hit.getSourceAsMap.get("second").toString
              level10 = hit.getSourceAsMap.get("third").toString
              level11 = hit.getSourceAsMap.get("fourth").toString
              level12 = hit.getSourceAsMap.get("fifth").toString
              level13 = hit.getSourceAsMap.get("sixth").toString
              level14 = hit.getSourceAsMap.get("seventh").toString
            } catch {
              case e:Exception => logger.error("安装PanDuanLevelEqualsFunction查询quanxianlevel_wangdianlevel_v1销售 ES表异常"+e.getMessage)
            }
          }

          val cxqyfwbh: String = wangdianbiao.get("cxqyfwbh").asText()
          val length: Int = (cxqyfwbh.length/8)
          val result: util.ArrayList[String] = SplitUtil.SplitFunctionFor8(cxqyfwbh)

          //将cxqyfw分离出分别对应各自层级
          for (i <- 0 until length){
            if(i==0){
              compar8 = result.get(0)
              logger.info("compar8为"+compar8)
            }
            if(i==1){
              compar9 = result.get(1)
              logger.info("compar9为"+compar9)
            }
            if(i==2){
              compar10 = result.get(2)
              logger.info("compar10为"+compar10)
            }
            if(i==3){
              compar11 = result.get(3)
              logger.info("compar11为"+compar11)
            }
            if(i==4){
              compar12 = result.get(4)
              logger.info("compar12为"+compar12)
            }
            if(i==5){
              compar13 = result.get(5)
              logger.info("compar6为"+compar13)
            }
            if(i==6){
              compar14 = result.get(6)
              logger.info("compar14为"+compar14)
            }
          }

          //判断两者层级是否完全相同
          if (level8.equals(compar8)&&level9.equals(compar9)&&level10.equals(compar10)&&level11.equals(compar11)&&level12.equals(compar12)&&level13.equals(compar13)&&level14.equals(compar14)){
            logger.info("安装销售层级关系相同不做处理")
          }else{

            try {
              ctx.output[WangdianLevel](wdLevelData,WangdianLevel(
                wangdianbiao.get("id").asText(),
                wangdianbiao.get("created_by").asText(),
                wangdianbiao.get("created_date").asText(),
                wangdianbiao.get("last_modified_by").asText(),
                wangdianbiao.get("last_modified_date").asText(),
                wangdianbiao.get("xhid").asText(),
                wangdianbiao.get("xtwdbh").asText(),
                wangdianbiao.get("wdno").asText(),
                wangdianbiao.get("splb").asText(),
                wangdianbiao.get("scqy").asText(),
                wangdianbiao.get("fwlb").asText(),
                wangdianbiao.get("sjwdno").asText(),
                wangdianbiao.get("sjwdmc").asText(),
                wangdianbiao.get("sjwdxtbh").asText(),
                wangdianbiao.get("cxqyfw").asText(),
                wangdianbiao.get("stat").asText(),
                wangdianbiao.get("czren").asText(),
                wangdianbiao.get("czrmc").asText(),
                wangdianbiao.get("czsj").asText(),
                wangdianbiao.get("zhczsj").asText(),
                wangdianbiao.get("cxqyfwbh").asText(),
                wangdianbiao.get("sfxyzq").asText(),
                wangdianbiao.get("sfxyzqbak").asText(),
                wangdianbiao.get("seno").asText(),
                wangdianbiao.get("leix").asText(),
                wangdianbiao.get("wdmc").asText(),
                wangdianbiao.get("spmc").asText(),
                wangdianbiao.get("scqymc").asText(),
                compar8,
                compar9,
                compar10,
                compar11,
                compar12,
                compar13,
                compar14
              ))

            } catch {
              case e: Exception => logger.error("安装销售PanDuanLevelEqualsFunction抓取数据异常->" + e.getMessage)
            }
          }
        }

      }
    ESTransportPoolUtil.returnClient(client)
  }
}
