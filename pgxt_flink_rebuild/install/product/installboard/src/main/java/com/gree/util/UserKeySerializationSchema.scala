package com.gree.util

import java.util

import com.google.gson.Gson
import com.gree.model.AzKafkaBuidData
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

class UserKeySerializationSchema extends KeyedSerializationSchema[AzKafkaBuidData]{
  override def serializeKey(element: AzKafkaBuidData): Array[Byte] = {
    val map = new util.HashMap[String,String]()
    val gosn:Gson = new Gson
    map.put("id",element.id)
    gosn.toJson(map).getBytes()
  }

  override def serializeValue(element: AzKafkaBuidData): Array[Byte] = {
    val fields = element.getClass.getDeclaredFields
    val map = new util.HashMap[String,Any](fields.length)
    val gosn:Gson = new Gson

    for (f <- fields){
      f.setAccessible(true)
      map.put(f.getName,f.get(element))
    }

    gosn.toJson(map).getBytes()
  }

  override def getTargetTopic(element: AzKafkaBuidData): String = {
    null
  }
}
