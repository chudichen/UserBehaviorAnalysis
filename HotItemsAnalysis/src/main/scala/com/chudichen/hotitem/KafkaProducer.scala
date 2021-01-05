package com.chudichen.hotitem

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * 消费生产者
 *
 * @author chudichen
 * @since 2020-12-29
 */
object KafkaProducer {

  def main(args: Array[String]): Unit = {
    writeToKafka("hotItems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String] (properties)
    // 从文件中读取数据发送
    val bufferedSource = io.Source.fromFile("/home/chu/IdeaProjects/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")

    bufferedSource.getLines.foreach(line => {
      println(line)
      val record = new ProducerRecord[String, String] (topic, line)
      producer.send(record)
    })
    producer.close()
  }
}
