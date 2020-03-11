package com.john.warehouse.sellcourse.dao

import org.apache.spark.sql.SparkSession

object DwsSellCourseDao {
  def getDwdSaleCourse(spark: SparkSession) = {
    spark.sql("select courseid,coursename,status,pointlistid,majorid,chapterid,chaptername,edusubjectid," +
      "edusubjectname,teacherid,teachername,coursemanager,money,dt,dn from dwd.dwd_sale_course")
  }
  def getDwdCourseShoppingCart(spark: SparkSession) = {
    spark.sql("select courseid,orderid,coursename,discount,sellmoney,createtime,dt,dn from dwd.dwd_course_shopping_cart")
  }
  def getDwdCourseShoppingCart2(spark: SparkSession) = {
    spark.sql("select courseid,orderid,coursename,discount,sellmoney,createtime,dt,dn from dwd.dwd_course_shopping_cart_cluster")
  }

  def getDwdCoursePay(spark: SparkSession) = {
    spark.sql("select orderid,discount,paymoney,createtime,dt,dn from dwd.dwd_course_pay")
  }
  def getDwdCoursePay2(spark: SparkSession) = {
    spark.sql("select orderid,discount,paymoney,createtime,dt,dn from dwd.dwd_course_pay_cluster")
  }
}
