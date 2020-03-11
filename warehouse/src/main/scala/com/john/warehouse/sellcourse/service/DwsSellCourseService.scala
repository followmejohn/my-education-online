package com.john.warehouse.sellcourse.service

import java.sql.Timestamp

import com.john.warehouse.bean.{DwdCourseShoppingCart, DwdSaleCourse}
import com.john.warehouse.sellcourse.dao.DwsSellCourseDao
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DwsSellCourseService {
  def importSellCourseDetail(spark: SparkSession, dt: String): Unit ={
    val dwdSaleCourse = DwsSellCourseDao.getDwdSaleCourse(spark).where(s"dt='${dt}'")
    val dwdCourseShoppingCart = DwsSellCourseDao.getDwdCourseShoppingCart(spark).where(s"dt='${dt}'")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay = DwsSellCourseDao.getDwdCoursePay(spark).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    dwdSaleCourse.join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }
  def importSellCourseDetail2(spark: SparkSession, dt: String) = {
    //解决数据倾斜 问题  将小表进行扩容 大表key加上随机散列值
    import spark.implicits._
    val coursePay= DwsSellCourseDao.getDwdCoursePay(spark).where(s"dt='${dt}'")
      .withColumnRenamed("discount","pay_discount")
      .withColumnRenamed("createtime","pay_createtime")
    val shoppingCart= DwsSellCourseDao.getDwdCourseShoppingCart(spark).where(s"dt='${dt}'")
      .withColumnRenamed("discount","cart_discount")
      .withColumnRenamed("createtime","cart_createtime")
    //大表加随机值
    val shoppingCartLater: Dataset[DwdCourseShoppingCart] = shoppingCart.mapPartitions(p => {
      p.map(it => {
        val courseid: Int = it.getAs[Int]("courseid")
        val randInt: Int = Random.nextInt(100)
        DwdCourseShoppingCart(courseid, it.getAs[String]("orderid"),
          it.getAs[String]("coursename"), it.getAs[java.math.BigDecimal]("cart_discount"),
          it.getAs[java.math.BigDecimal]("sellmoney"), it.getAs[Timestamp]("cart_createtime"),
          it.getAs[String]("dt"), it.getAs[String]("dn"), courseid + "_" + randInt)
      })
    })
    //小表扩容
    val course: Dataset[Row] = DwsSellCourseDao.getDwdSaleCourse(spark).where(s"dt='${dt}'")
    val courseLater: Dataset[DwdSaleCourse] = course.flatMap(item => {
      val list = new ArrayBuffer[DwdSaleCourse]()
      val courseid = item.getAs[Int]("courseid")
      val coursename = item.getAs[String]("coursename")
      val status = item.getAs[String]("status")
      val pointlistid = item.getAs[Int]("pointlistid")
      val majorid = item.getAs[Int]("majorid")
      val chapterid = item.getAs[Int]("chapterid")
      val chaptername = item.getAs[String]("chaptername")
      val edusubjectid = item.getAs[Int]("edusubjectid")
      val edusubjectname = item.getAs[String]("edusubjectname")
      val teacherid = item.getAs[Int]("teacherid")
      val teachername = item.getAs[String]("teachername")
      val coursemanager = item.getAs[String]("coursemanager")
      val money = item.getAs[java.math.BigDecimal]("money")
      val dt = item.getAs[String]("dt")
      val dn = item.getAs[String]("dn")
      for (i <- 0 until 100) {
        list.append(DwdSaleCourse(courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
          edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, courseid + "_" + i))
      }
      list.toIterator
    })
    courseLater.join(shoppingCartLater.drop("courseid").drop("coursename"),Seq("rand_courseid","dt","dn"),"right")
      .join(coursePay,Seq("orderid","dt","dn"),"left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }
  def importSellCourseDetail3(sparkSession: SparkSession, dt: String) = {
    //解决数据倾斜问题 采用广播小表
    val dwdSaleCourse = DwsSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
    val dwdCourseShoppingCart = DwsSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt='${dt}'")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay = DwsSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import org.apache.spark.sql.functions._
    broadcast(dwdSaleCourse).join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }
  def importSellCourseDetail4(sparkSession: SparkSession, dt: String) = {
    //解决数据倾斜问题 采用广播小表  两个大表进行分桶并且进行SMB join，join以后的大表再与广播后的小表进行join
    val dwdSaleCourse = DwsSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
    val dwdCourseShoppingCart = DwsSellCourseDao.getDwdCourseShoppingCart2(sparkSession).where(s"dt='${dt}'")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay = DwsSellCourseDao.getDwdCoursePay2(sparkSession).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import org.apache.spark.sql.functions._
    val tmpdata = dwdCourseShoppingCart.join(dwdCoursePay, Seq("orderid"), "left")
    val result = broadcast(dwdSaleCourse).join(tmpdata, Seq("courseid"), "right")
    result.select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
      , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
      "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dwd.dwd_sale_course.dt", "dwd.dwd_sale_course.dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }
}
