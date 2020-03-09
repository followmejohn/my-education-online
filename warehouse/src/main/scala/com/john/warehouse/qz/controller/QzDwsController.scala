package com.john.warehouse.qz.controller

import com.john.warehouse.qz.service.QzDwsService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object QzDwsController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("qzDws")//.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//    val ssc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    QzDwsService.dwsQzChapter(spark,"20190722")
    QzDwsService.dwsQzCourse(spark,"20190722")
    QzDwsService.dwsQzMajor(spark,"20190722")
    QzDwsService.dwsQzPaper(spark,"20190722")
    QzDwsService.dwsQzQuestion(spark,"20190722")
    QzDwsService.dwsUserPaperDetail(spark,"20190722")
  }
}
