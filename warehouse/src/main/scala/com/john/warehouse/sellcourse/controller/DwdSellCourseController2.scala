package com.john.warehouse.sellcourse.controller

import com.john.warehouse.sellcourse.service.DwdSellCourseService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DwdSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sell")//.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    DwdSellCourseService.importDwdSellCourse(sc, spark)
    //dwd分桶表
    DwdSellCourseService.importCoursePay2(sc, spark)
    DwdSellCourseService.importCourseShoppingCart2(sc, spark)
  }
}
