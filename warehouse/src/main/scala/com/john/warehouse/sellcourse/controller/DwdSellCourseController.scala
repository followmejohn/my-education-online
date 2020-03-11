package com.john.warehouse.sellcourse.controller

import com.john.warehouse.sellcourse.service.DwdSellCourseService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdSellCourseController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sell")//.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    DwdSellCourseService.importDwdSellCourse(sc, spark)
    DwdSellCourseService.importCoursePay(sc, spark)
    DwdSellCourseService.importCourseShoppingCart(sc, spark)
  }
}
