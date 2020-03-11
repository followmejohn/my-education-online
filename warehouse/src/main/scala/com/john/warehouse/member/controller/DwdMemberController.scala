package com.john.warehouse.member.controller

import com.john.warehouse.member.service.EtlDataService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdMemberController {
  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "john")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dwdMemberImport")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    EtlDataService.etlMember(sc, spark)
    EtlDataService.etlBaseAd(sc, spark)
    EtlDataService.etlBaseWebsite(sc, spark)
    EtlDataService.etlVipLevel(sc, spark)
    EtlDataService.etlMemberRegtype(sc, spark)
    EtlDataService.etlPcentermempaymoney(sc, spark)
  }
}
