package com.john.warehouse.qz.controller

import com.john.warehouse.qz.service.QzDwdService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object QzDwdController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("qz").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    QzDwdService.getQzChapter(ssc,spark)
    QzDwdService.getQzChapterList(ssc,spark)
    QzDwdService.getQzPoint(ssc,spark)
    QzDwdService.getQzPointQuestion(ssc,spark)
    QzDwdService.getQzSiteCourse(ssc,spark)
    QzDwdService.getQzCourseEduSubject(ssc,spark)
    QzDwdService.getQzCourse(ssc,spark)
    QzDwdService.getQzWebSite(ssc,spark)
    QzDwdService.getQzMajor(ssc,spark)
    QzDwdService.getQzBusiness(ssc,spark)
    QzDwdService.getQzPaperView(ssc,spark)
    QzDwdService.getQzPaper(ssc,spark)
    QzDwdService.getQzCenterPaper(ssc,spark)
    QzDwdService.getQzCenter(ssc,spark)
    QzDwdService.getQzQuestionType(ssc,spark)
    QzDwdService.getQzQuestion(ssc,spark)
    QzDwdService.getQzMemberPaperQuestion(ssc,spark)
  }
}
