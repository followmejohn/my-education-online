package com.john.warehouse.qz.dao

import org.apache.spark.sql.SparkSession

object QzAdsDao {
  //统计各试卷平均耗时 平均分
  def avgScoreAndTime(spark: SparkSession, dt: String)={
    spark.sql(
      s"""
        |select
        |   paperviewid,
        |   paperviewname,
        |   cast(avg(score) as decimal(4,1)) score,
        |   cast(avg(spendtime) as decimal(10,2)) spendtime,
        |   dt,
        |   dn
        |from dws.dws_user_paper_detail
        |where dt = '$dt'
        |group by paperviewid,paperviewname,dt,dn
        |order by score desc, spendtime desc
      """.stripMargin)
  }
  //统计各试卷最高分、最低分
  def scoreHighAndLow(spark: SparkSession, dt: String)={
    spark.sql(s"""select
                        |    paperviewid,
                        |    paperviewname,
                        |    cast(max(score) as decimal(4,1)),
                        |    cast(min(score) as decimal(4,1)),
                        |    dt,
                        |    dn
                        |from dws.dws_user_paper_detail
                        |where dt='$dt'
                        |group by paperviewid,paperviewname,dt,dn""".stripMargin)
  }
  //按试卷分组统计每份试卷的前三用户详情
  def paparTop3(spark: SparkSession, dt: String) ={
   spark.sql(
     s"""
       |select *
       |from
       |  (select
       |      userid,paperviewid,paperviewname,chaptername,pointname,
       |          sitecoursename,coursename,majorname,
       |          shortname,papername,score,
       |      row_number() over(partition by paperviewid order by score desc) as rn,
       |      dt,
       |      dn
       |  from dws.dws_user_paper_detail
       |  where dt = '${dt}'
       |  ) t1
       |where rn < 4
     """.stripMargin)
  }
  //倒数前三
  def paperLow3(spark: SparkSession, dt: String)={
    spark.sql(s"""select *
                        |from
                        |    (
                        |     select
                        |        userid,paperviewid,paperviewname,chaptername,pointname,
                        |        sitecoursename,coursename,majorname,
                        |        shortname,papername,score,
                        |        dense_rank() over (partition by paperviewid order by score asc) as rk,
                        |        dt,dn
                        |     from dws.dws_user_paper_detail
                        |     where dt='$dt'
                        |    ) t1
                        |where rk<4""".stripMargin)
  }
  //统计各试卷各分段的用户id，分段有0-20,20-40,40-60,60-80,80-100
  def stageDistribution(spark: SparkSession, dt: String)={
    spark.sql(
      s"""
        |select
        |   paperviewid,
        |   paperviewname,
        |   score_segment,
        |   concat_ws(',',collect_list(cast(userid as string))),
        |   dt,dn
        |from
        |   (select
        |      paperviewid,
        |      paperviewname,
        |      userid,
        |      case when score >= 0 and score < 20 then '0-20'
        |          when score >= 20 and score < 40 then '20-40'
        |          when score >=40 and score < 60 then '40-60'
        |          when score >=60 and score < 80 then '60-80'
        |          when score >=80 and score <=100 then '80-100'
        |       end as score_segment,
        |        dt,
        |        dn
        |   from dws.dws_user_paper_detail
        |   where dt='${dt}') t1
        |group by paperviewid,paperviewname,score_segment,dt,dn
        |order by paperviewid,score_segment
      """.stripMargin)
  }
  //统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
  def paperPassOrNo(spark: SparkSession, dt: String) ={
    spark.sql(
      s"""
        |select
        |   paperviewid,
        |   paperviewname,
        |   unpasscount,
        |   passcount,
        |   cast((passcount/(passcount + unpasscount)) as decimal(4,2)) rate,
        |   dt,dn
        |from
        |   (select
        |      paperviewid,
        |      paperviewname,
        |      sum(if(score < 60,1,0)) unpasscount,
        |      sum(if(score >= 60,1,0)) passcount,
        |      dt,dn
        |   from dws.dws_user_paper_detail
        |   where dt = '${dt}'
        |   group by paperviewid,paperviewname,dt,dn) t
        |order by paperviewid
      """.stripMargin)
  }
  //统计各题的错误数，正确数，错题率
  def questionCount(spark: SparkSession, dt: String) ={
    spark.sql(
      s"""
        |select
        |   questionid,
        |   errcount,
        |   rightcount,
        |   cast((rightcount/(rightcount + errcount)) as decimal(4,2)) rate,
        |   dt,dn
        |from
        |   (select
        |      questionid,
        |      sum(if(user_question_answer = '0',1,0)) errcount,
        |      sum(if(user_question_answer = '1',1,0)) rightcount,
        |      dt,dn
        |   from dws.dws_user_paper_detail
        |   where dt = '${dt}'
        |   group by questionid,dt,dn) t
        |order by questionid
      """.stripMargin)
  }
}


























