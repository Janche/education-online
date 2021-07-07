package com.atguigu.util

import org.apache.spark.sql.SparkSession

object HiveUtil {

  /**
    * 调大最大分区个数
    * @param spark
    * @return
    */
   def setMaxpartitions(spark: SparkSession)={
     //开启动态分区功能
     spark.sql("set hive.exec.dynamic.partition=true")
     //动态分区的模式设置为非严格模式
     spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
     //在所有执行MR的节点上，最大一共可以创建多少个动态分区，默认值1000
     spark.sql("set hive.exec.max.dynamic.partitions=100000")
     //在每个执行MR的节点上，最大可以创建多少个动态分区，默认值100
     spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
     //整个MR Job中，最大可以创建多少个HDFS文件，默认值100000
     spark.sql("set hive.exec.max.created.files=100000")
   }
  /**
    * 开启压缩
    *
    * @param spark
    * @return
    */
  def openCompression(spark: SparkSession) = {
    spark.sql("set mapred.output.compress=true")
    spark.sql("set hive.exec.compress.output=true")
  }

  /**
    * 开启动态分区，非严格模式
    *
    * @param spark
    */
  def openDynamicPartition(spark: SparkSession) = {
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }

  /**
    * 使用lzo压缩
    *
    * @param spark
    */
  def useLzoCompression(spark: SparkSession) = {
    spark.sql("set io.compression.codec.lzo.class=com.hadoop.compression.lzo.LzoCodec")
    spark.sql("set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec")
  }

  /**
    * 使用snappy压缩
    * @param spark
    */
  def useSnappyCompression(spark:SparkSession)={
    spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
    spark.sql("set mapreduce.output.fileoutputformat.compress=true")
    spark.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
  }

}
