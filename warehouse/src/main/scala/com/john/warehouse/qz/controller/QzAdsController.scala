package com.john.warehouse.qz.controller

import com.john.warehouse.qz.service.QzAdsService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object QzAdsController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("adsQz")//.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    QzAdsService.getResult(spark,"20190722")
  }
}
