package com.atguigu.qz.controller

import org.apache.spark.sql.SparkSession

object SparkDemo {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("Spark to Hive")
            .master("local[4]")
            .config("hive.metastore.uris", "thrift://hadoop101:9083")
            .enableHiveSupport()
            .getOrCreate()

        val df = spark.sql("select * from dwd.dwd_base_ad")
        df.show(false)

        spark.close()
    }
}
