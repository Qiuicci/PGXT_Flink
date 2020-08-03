package com.gree.func

import com.gree.model.TrueData
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

class MyPartitioner extends FlinkKafkaPartitioner[TrueData]{
  override def partition(record: TrueData, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
    //这里接收到的key是上面MySchema()中序列化后的key，需要转成string，然后取key的hash值`%`上kafka分区数量
    val i: Int = Math.abs(new String(key).hashCode() % partitions.length)
    println("key为"+new String(key)+"对应分区为->"+i+"分区数为->"+partitions.length)
    i
  }
}
