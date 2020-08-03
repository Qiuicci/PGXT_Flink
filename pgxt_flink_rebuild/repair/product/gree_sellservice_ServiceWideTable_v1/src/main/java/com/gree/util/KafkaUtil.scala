package com.gree.util

import java.util.Properties

import org.apache.flink.streaming.connectors.kafka.internal.KafkaFetcher
import com.gree.constant.Constant
import com.gree.func.{MyPartitioner, UserKeyedSerializationSchema}
import com.gree.model.TrueData
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig



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
  prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,conf.getString(Constant.KAFKA_HOST))
  prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"100")
  prop.setProperty(ProducerConfig.LINGER_MS_CONFIG,"500")
  prop.setProperty(ProducerConfig.ACKS_CONFIG,"all")
  prop.setProperty(ProducerConfig.RETRIES_CONFIG,"2")
//  prop.setProperty("auto.offset.reset",Constant.KAFKA_AUTO_OFFSET_RESET)
  /**
    * 获取Kafka连接消费者
    */
  def getKafkaConnect(KafkaTopic:String,fsEnv:StreamExecutionEnvironment):DataStream[ObjectNode] = {
    val value:DataStream[ObjectNode]= fsEnv.addSource(new FlinkKafkaConsumer010(KafkaTopic, new JSONKeyValueDeserializationSchema(true), properties))

    //val value:DataStream[ObjectNode]= fsEnv.addSource(new )
    value
  }

  /**
    *
    * 连接kafka作为生产者
    *
    */
  def getKfakaProducer(Kafkatopic:String):FlinkKafkaProducer010[TrueData]={
    val value = new FlinkKafkaProducer010[TrueData](
      Kafkatopic,
      new UserKeyedSerializationSchema,
      prop,
      new MyPartitioner
    )
    value
  }
}
