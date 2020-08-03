package com.gree.func

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass


class MyJSONKeyValueDeserializationSchema(includeMetadata:Boolean)  extends  KafkaDeserializationSchema[ObjectNode]{
  private val serialVersionUID = 1509391548173891955L
  private var mapper = new ObjectMapper()

  @throws[Exception]
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ObjectNode = {
    if (mapper == null) {
      mapper = new ObjectMapper
    }
    val node = mapper.createObjectNode
    if (record.key != null){
      node.set("key", mapper.readValue(new String(record.key), classOf[JsonNode]))
    }
    if (record.value != null) {
      node.set("value", mapper.readValue(record.value, classOf[JsonNode]))
    }
    if (includeMetadata) {
      node.putObject("metadata").put("offset", record.offset).put("topic", record.topic).put("partition", record.partition)
    }
    node
  }

  override def isEndOfStream(nextElement: ObjectNode) = {
    false
  }

  override def getProducedType: TypeInformation[ObjectNode] = {
    getForClass(classOf[ObjectNode])
  }
}
