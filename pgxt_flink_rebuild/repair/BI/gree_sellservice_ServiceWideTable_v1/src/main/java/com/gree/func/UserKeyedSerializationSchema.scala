package com.gree.func

import java.util

import com.google.gson.{Gson, JsonArray, JsonObject}
import com.gree.model.TrueData
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

class UserKeyedSerializationSchema extends  KeyedSerializationSchema[TrueData]{
  override def serializeKey(element: TrueData): Array[Byte] = {
    val map = new util.HashMap[String, String](4)
    val gosn: Gson = new Gson()
    val arr: util.ArrayList[util.HashMap[String, String]] = new util.ArrayList[util.HashMap[String, String]]()
    map.put("id", element.id)
    arr.add(map)
    //map类型转换成json数据格式
    val jsonstr1 = gosn.toJson(map)
    jsonstr1.getBytes()
  }

  override def serializeValue(element: TrueData): Array[Byte] = {
    //把数据换成json格式输出后写入kafka。
    //把数据换成json格式输出后写入kafka。(kafka消费list数据报错)
    val map = new util.HashMap[String, String](4)
    val gosn: Gson = new Gson()
    val arr: util.ArrayList[util.HashMap[String, String]] = new util.ArrayList[util.HashMap[String, String]](4)
    map.put("id", element.id)
    map.put("pgid", element.pgid)
    map.put("table", element.table)
    map.put("ts", element.ts)
    arr.add(map)
    //map类型转换成json数据格式
    val jsonstr = gosn.toJson(map)
    jsonstr.getBytes()
  }

  override def getTargetTopic(element: TrueData): String = {
   null
  }
}
