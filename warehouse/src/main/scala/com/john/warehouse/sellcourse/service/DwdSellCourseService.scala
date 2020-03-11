package com.john.warehouse.sellcourse.service

import com.alibaba.fastjson.JSONObject
import com.john.warehouse.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object DwdSellCourseService {
  def importDwdSellCourse(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val rdd: RDD[String] = sc.textFile("/user/john/ods/salecourse.log")
    rdd.filter(it =>{
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(p=>{
      p.map(it=>{
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val courseid = jsonObject.getString("courseid")
        val coursename = jsonObject.getString("coursename")
        val status = jsonObject.getString("status")
        val pointlistid = jsonObject.getString("pointlistid")
        val majorid = jsonObject.getString("majorid")
        val chapterid = jsonObject.getString("chapterid")
        val chaptername = jsonObject.getString("chaptername")
        val edusubjectid = jsonObject.getString("edusubjectid")
        val edusubjectname = jsonObject.getString("edusubjectname")
        val teacherid = jsonObject.getString("teacherid")
        val teachername = jsonObject.getString("teachername")
        val coursemanager = jsonObject.getString("coursemanager")
        val money = jsonObject.getString("money")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, coursename, status, pointlistid, majorid, chapterid, chaptername,
          edusubjectid, edusubjectname, teacherid, teachername, coursemanager, money, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_sale_course")
  }
  def importCoursePay(sc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    sc.textFile("/user/john/ods/coursepay.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val orderid = jsonObject.getString("orderid")
        val paymoney = jsonObject.getString("paymoney")
        val discount = jsonObject.getString("discount")
        val createtime = jsonObject.getString("createitme")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (orderid, discount, paymoney, createtime, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_course_pay")
  }
  def importCoursePay2(sc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    sc.textFile("/user/john/ods/coursepay.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val orderid = jsonObject.getString("orderid")
        val paymoney = jsonObject.getString("paymoney")
        val discount = jsonObject.getString("discount")
        val createtime = jsonObject.getString("createitme")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (orderid, discount, paymoney, createtime, dt, dn)
      })
    }).toDF("orderid", "discount", "paymoney", "createtime", "dt", "dn").
      write.partitionBy("dt", "dn").
      bucketBy(10, "orderid").sortBy("orderid").
      mode(SaveMode.Overwrite).saveAsTable("dwd.dwd_course_pay_cluster")
  }

  def importCourseShoppingCart(sc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    sc.textFile("/user/john/ods/courseshoppingcart.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val courseid = jsonObject.getString("courseid")
        val orderid = jsonObject.getString("orderid")
        val coursename = jsonObject.getString("coursename")
        val discount = jsonObject.getString("discount")
        val sellmoney = jsonObject.getString("sellmoney")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, orderid, coursename, discount, sellmoney, createtime, dt, dn)
      })
    }).toDF().coalesce(6).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_course_shopping_cart")
  }

  def importCourseShoppingCart2(sc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    sc.textFile("/user/john/ods/courseshoppingcart.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val courseid = jsonObject.getString("courseid")
        val orderid = jsonObject.getString("orderid")
        val coursename = jsonObject.getString("coursename")
        val discount = jsonObject.getString("discount")
        val sellmoney = jsonObject.getString("sellmoney")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, orderid, coursename, discount, sellmoney, createtime, dt, dn)
      })
    }).toDF("courseid", "orderid", "coursename", "discount", "sellmoney", "createtime", "dt", "dn")
      .write.partitionBy("dt", "dn").
      bucketBy(10, "orderid").sortBy("orderid").
      mode(SaveMode.Overwrite).saveAsTable("dwd.dwd_course_shopping_cart_cluster")
  }
}
