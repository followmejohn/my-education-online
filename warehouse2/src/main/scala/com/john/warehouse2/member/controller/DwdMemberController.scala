package com.john.warehouse2.member.controller

import com.john.warehouse2.util.HiveUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DwdMemberController {
  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "john")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dwdMemberImport")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
  }
}
