package com.john.warehouse.qz.dao

import org.apache.spark.sql.SparkSession

object QzDwsChapterDao {
  def getDwdQzChapter(spark: SparkSession, dt: String)={
    spark.sql("select chapterid,chapterlistid,chaptername,sequence,showstatus,creator as " +
      "chapter_creator,createtime as chapter_createtime,courseid as chapter_courseid,chapternum,outchapterid,dt,dn from dwd.dwd_qz_chapter where " +
      s"dt='$dt'")
  }
  def getDwdQzChapterList(spark: SparkSession, dt: String)={
    spark.sql("select chapterlistid,chapterlistname,chapterallnum,dn from dwd.dwd_qz_chapter_list " +
      s"where dt='$dt'")
  }
  def getDwdQzPoint(spark: SparkSession, dt: String)={
    spark.sql("select pointid,pointname,pointyear,chapter,excisenum,pointlistid,chapterid," +
      "pointdescribe,pointlevel,typelist,score as point_score,thought,remid,pointnamelist,typelistids,pointlist,dn from " +
      s"dwd.dwd_qz_point where dt='$dt'")
  }
  def getDwdQzPointQuestion(spark: SparkSession, dt: String)={
    spark.sql(s"select pointid,questionid,questype,dn from dwd.dwd_qz_point_question where dt='$dt'")
  }
}
