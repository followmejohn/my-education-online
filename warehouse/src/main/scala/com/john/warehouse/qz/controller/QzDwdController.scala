package com.john.warehouse.qz.controller

import com.john.warehouse.qz.service.QzDwdService
import com.john.warehouse.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object QzDwdController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("qz")//.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    HiveUtil.openDynamicPartition(spark)
    QzDwdService.getQzChapter(sc,spark)
    QzDwdService.getQzChapterList(sc,spark)
    QzDwdService.getQzPoint(sc,spark)
    QzDwdService.getQzPointQuestion(sc,spark)
    QzDwdService.getQzSiteCourse(sc,spark)
    QzDwdService.getQzCourseEduSubject(sc,spark)
    QzDwdService.getQzCourse(sc,spark)
    QzDwdService.getQzWebSite(sc,spark)
    QzDwdService.getQzMajor(sc,spark)
    QzDwdService.getQzBusiness(sc,spark)
    QzDwdService.getQzPaperView(sc,spark)
    QzDwdService.getQzPaper(sc,spark)
    QzDwdService.getQzCenterPaper(sc,spark)
    QzDwdService.getQzCenter(sc,spark)
    QzDwdService.getQzQuestionType(sc,spark)
    QzDwdService.getQzQuestion(sc,spark)
    QzDwdService.getQzMemberPaperQuestion(sc,spark)
  }
}
