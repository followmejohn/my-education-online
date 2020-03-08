package com.john.warehouse.member.controller

import com.john.warehouse.member.service.DwsMemberService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsMemberController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dws_member").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    DwsMemberService.importMember(spark, "20190722")
  }
}
