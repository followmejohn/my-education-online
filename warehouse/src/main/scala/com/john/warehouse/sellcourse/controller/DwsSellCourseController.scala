package com.john.warehouse.sellcourse.controller

import com.john.warehouse.sellcourse.service.DwsSellCourseService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsSellCourseController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sale")//.setMaster("local[*]")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")//自动广播文件大小小于设定值的变量，设定值为-1相当于禁用自动广播
    //.set("spark.sql.shuffle.partitions", "15")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    DwsSellCourseService.importSellCourseDetail(spark,"20190722")
  }
}
