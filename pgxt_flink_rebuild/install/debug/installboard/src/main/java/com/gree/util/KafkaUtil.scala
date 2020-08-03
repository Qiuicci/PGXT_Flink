package com.gree.util

import java.util.Properties

import com.gree.constant.Constant
import com.gree.model.AzKafkaBuidData
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.flink.streaming.api.scala._


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

  /**
   * 获取Kafka连接
   */
  def getKafkaConnect(KafkaTopic:String,fsEnv:StreamExecutionEnvironment):DataStream[ObjectNode] = {
    val value:DataStream[ObjectNode]= fsEnv.addSource(new FlinkKafkaConsumer(KafkaTopic, new JSONKeyValueDeserializationSchema(true), properties))
    value
  }

  /**
   *kafka生产者
   *
  **/

  def getKafkaProducer(KafkaTopic:String):FlinkKafkaProducer010[AzKafkaBuidData] = {
    val value1 = new FlinkKafkaProducer010(KafkaTopic,new UserKeySerializationSchema(),prop,new AzKafkaPartitioner())
    value1
  }

}