package com.atguigu.sellcourse.controller

import com.atguigu.sellcourse.service.DwdSellCourseService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_sellcourse_import") //.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    //设置分桶相关参数
    //    sparkSession.sql("set hive.enforce.bucketing=false")
    //    sparkSession.sql("set hive.enforce.sorting=false")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    //    DwdSellCourseService.importSaleCourseLog(ssc, sparkSession)
    DwdSellCourseService.importCoursePay2(ssc, sparkSession)
    DwdSellCourseService.importCourseShoppingCart2(ssc, sparkSession)
  }
}
