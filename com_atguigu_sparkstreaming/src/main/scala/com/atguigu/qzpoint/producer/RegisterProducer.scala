package com.atguigu.qzpoint.producer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RegisterProducer {
  def test: Unit ={
    val sparkConf = new SparkConf().setMaster("local[*}").setAppName("registerProducer")
    val ssc = new SparkContext(sparkConf)
    val rdd: RDD[String] = ssc.textFile("file://" + this.getClass.getResource("/register.log").getPath, 10)
    rdd.foreachPartition(partition =>{
      val prop = new Properties()
      prop.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103::9092")
      prop.put("acks","1")
      prop.put("batch.size", "16384")
      prop.put("linger.ms", "10")
      prop.put("buffer.memory", "33554432")
      prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](prop)
      partition.foreach(item => {
        val msg = new ProducerRecord[String, String]("register", item)
        producer.send(msg)
      })
      producer.flush()
      producer.close()
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    ssc.textFile("file://"+this.getClass.getResource("/register.log").getPath, 10)
      .foreachPartition(partition => {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
        props.put("acks", "1")
        props.put("batch.size", "16384")
        props.put("linger.ms", "10")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item => {
          val msg = new ProducerRecord[String, String]("register_topic",item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
