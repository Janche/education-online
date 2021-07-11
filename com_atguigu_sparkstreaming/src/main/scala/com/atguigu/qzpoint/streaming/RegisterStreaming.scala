package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.ResultSet
import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RegisterStreaming {
  private val groupid = "register_group_test"

  def test()={
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext = ssc.sparkContext
    val topics = Array("register_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> false
    )
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    // sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val client = DataSourceUtil.getConnection
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()

    try{
      sqlProxy.executeQuery(client, "select * from offset_manager where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val topic = rs.getString(2)
            val partition = rs.getInt(3)
            val offset = rs.getLong(4)
            val topicPartition = new TopicPartition(topic, partition)
            offsetMap.put(topicPartition, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val dStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(topics, kafkaMap, offsetMap))
    }
    val resultDStream = dStream.filter(item => item.value().split("\t").length == 3)
      .mapPartitions(partition => {
        partition.map(item => {
          val line = item.value()
          val arr = line.split("\t")
          val appName = arr(0) match {
            case "1" => "PC"
            case "2" => "APP"
            case "_" => "Other"
          }
          (appName, 1)
        })
      })
    resultDStream.cache()
    resultDStream.reduceByKeyAndWindow((x,y)=> x+y, Seconds(60), Seconds(6))

   //"+++++++++++++++++++++++实时注册人数+++++++++++++++++++++++"
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum //本批次求和
      val previousCount = state.getOrElse(0) //历史数据
      Some(currentCount + previousCount)
    }
    resultDStream.updateStateByKey(updateFunc).print()
   //"++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

    dStream.foreachRDD(rdd =>{
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try{
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(item => {
          sqlProxy.executeUpdate(client, "replace into offset_manager (groupid, topic, partition, untilOffset) values(?,?,?,?)",
            Array(groupid, item.topic, item.partition, item.untilOffset))
        })
      }catch {
        case e: Exception => e.printStackTrace()
      }finally {
        sqlProxy.shutdown(client)
      }
    })Ø
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
//      .set("spark.streaming.backpressure.enabled", "true")
//      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext: SparkContext = ssc.sparkContext
    val topics = Array("register_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    //sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    val resultDStream = stream.filter(item => item.value().split("\t").length == 3).
      mapPartitions(partitions => {
        partitions.map(item => {
          val line = item.value()
          val arr = line.split("\t")
          val app_name = arr(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (app_name, 1)
        })
      })
    resultDStream.cache()
    //(PC,1),(PC,1),(APP,1),(Other,1),(APP,1),(Other,1),(PC,1),(APP,1)
    //"=================每6s间隔1分钟内的注册数据================="
    resultDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()
    //"========================================================="

    //"+++++++++++++++++++++++实时注册人数+++++++++++++++++++++++"
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum //本批次求和
      val previousCount = state.getOrElse(0) //历史数据
      Some(currentCount + previousCount)
    }
    resultDStream.updateStateByKey(updateFunc).print()
    //"++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


    //    val dsStream = stream.filter(item => item.value().split("\t").length == 3)
    //      .mapPartitions(partitions =>
    //        partitions.map(item => {
    //          val rand = new Random()
    //          val line = item.value()
    //          val arr = line.split("\t")
    //          val app_id = arr(1)
    //          (rand.nextInt(3) + "_" + app_id, 1)
    //        }))
    //    val result = dsStream.reduceByKey(_ + _)
    //    result.map(item => {
    //      val appid = item._1.split("_")(1)
    //      (appid, item._2)
    //    }).reduceByKey(_ + _).print()

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    
    ssc.start()
    ssc.awaitTermination()
  }

}
