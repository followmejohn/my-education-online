package com.john.warehouse.util

import org.apache.spark.sql.SparkSession

object HiveUtil {
  //设置动态分区及非严格模式
  def openDynamicPartition(spark: SparkSession) = {
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }
  //开启压缩
  def openCompression(spark: SparkSession) = {
    spark.sql("set mapred.output.compress=true")
    spark.sql("set hive.exec.compress.output=true")
  }
}
