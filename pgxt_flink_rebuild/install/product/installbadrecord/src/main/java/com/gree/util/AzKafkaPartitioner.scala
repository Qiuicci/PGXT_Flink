package com.gree.util

import com.gree.model.AzKafkaBuidData
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

class AzKafkaPartitioner extends FlinkKafkaPartitioner[AzKafkaBuidData]{
  override def partition(element: AzKafkaBuidData, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
    val i : Int = Math.abs(key.hashCode % partitions.length)
    //println("key : " + new String(key) + "   对应的分区为 ： " + i + "  分区总数为 ： " + partitions.length )
    i
  }
}
