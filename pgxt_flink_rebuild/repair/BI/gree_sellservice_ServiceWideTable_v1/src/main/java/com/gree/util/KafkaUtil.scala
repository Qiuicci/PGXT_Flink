package com.gree.util

import java.util.Properties

import com.gree.constant.Constant
import com.gree.func.{MyPartitioner, UserKeyedSerializationSchema}
import com.gree.model.TrueData
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig


/**
  * KAFKA工具类
  */
object KafkaUtil {

  val conf = ConfigurationUtil(Constant.CONFIG_PROPERTIES)
  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString(Constant.KAFKA_HOST))
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constant.KAFKA_GROUP_ID)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,Constant.KAFKA_AUTO_OFFSET_RESET)
  /* properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Constant.KAFKA_BROKER_LIST)*/


  val prop = new Properties()
  prop.setProperty("bootstrap.servers",conf.getString(Constant.KAFKA_HOST))
  prop.setProperty("group.id",Constant.KAFKA_GROUP_ID)
  /**
    * 获取Kafka连接消费者
    */
  def getKafkaConnect(KafkaTopic:String,fsEnv:StreamExecutionEnvironment):DataStream[ObjectNode] = {
    val value:DataStream[ObjectNode]= fsEnv.addSource(new FlinkKafkaConsumer(KafkaTopic, new JSONKeyValueDeserializationSchema(true), properties))

    //val value:DataStream[ObjectNode]= fsEnv.addSource(new )
    value
  }

  /**
    *连接kafka作为生产者
    */
  def getKfakaProducer(KafkaTopic:String):FlinkKafkaProducer010[TrueData] = {
    val value = new FlinkKafkaProducer010[TrueData](
      KafkaTopic,
      new UserKeyedSerializationSchema,
      prop ,//sink到kafka的配置信息
      new MyPartitioner()
    )
    value
  }
}
