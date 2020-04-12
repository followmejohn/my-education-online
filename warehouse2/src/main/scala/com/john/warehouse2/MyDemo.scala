package com.john.warehouse2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("haha").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("")
    val value: RDD[Int] = rdd.map(it => {
      val lines: Array[String] = it.split("\\W+")
      val inta: Int = lines(0).toInt
      inta
    })

  }
}
