package com.john.warehouse.qz.service

import com.alibaba.fastjson.JSONObject
import com.john.warehouse.bean.{QzPageViewContainer, QzPointContainer, QzQuestionContainer}
import com.john.warehouse.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object QzDwdService {
  //test_git1
  //i have a dream
  def getQzChapter(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzChapter.log")
    sourceRdd.filter{it=>
      val obj= ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val chapterid= jsonObject.getIntValue("chapterid")
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        val chaptername = jsonObject.getString("chaptername")
        val sequence = jsonObject.getString("sequence")
        val showstatus = jsonObject.getString("showstatus")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val courseid = jsonObject.getIntValue("courseid")
        val chapternum = jsonObject.getIntValue("chapternum")
        val outchapterid = jsonObject.getIntValue("outchapterid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (chapterid, chapterlistid, chaptername, sequence, showstatus, creator, createtime,
          courseid, chapternum, outchapterid, dt, dn)
      }
    }.toDF().coalesce(2).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter")
  }
  def getQzChapterList(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzChapterList.log")
    sourceRdd.filter{it=>
      val obj= ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{ it =>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val chapterlistid= jsonObject.getIntValue("chapterlistid")
        val chapterlistname = jsonObject.getString("chapterlistname")
        val courseid = jsonObject.getIntValue("courseid")
        val chapterallnum = jsonObject.getIntValue("chapterallnum")
        val sequence = jsonObject.getString("sequence")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status, creator, createtime, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter_list")
  }
  def getQzPoint(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzPoint.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val pointid = jsonObject.getIntValue("pointid")
        val courseid = jsonObject.getIntValue("courseid")
        val pointname = jsonObject.getString("pointname")
        val pointyear = jsonObject.getString("pointyear")
        val chapter = jsonObject.getString("chapter")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val status = jsonObject.getString("status")
        val modifystatus = jsonObject.getString("modifystatus")
        val excisenum = jsonObject.getIntValue("excisenum")
        val pointlistid = jsonObject.getIntValue("pointlistid")
        val chapterid = jsonObject.getIntValue("chapterid")
        val sequence = jsonObject.getString("sequence")
        val pointdescribe = jsonObject.getString("pointdescribe")
        val pointlevel = jsonObject.getString("pointlevel")
        val typeslist = jsonObject.getString("typelist")
        val score = BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP) //保留1位小数 并四舍五入
      val thought = jsonObject.getString("thought")
        val remid = jsonObject.getString("remid")
        val pointnamelist = jsonObject.getString("pointnamelist")
        val typelistids = jsonObject.getString("typelistids")
        val pointlist = jsonObject.getString("pointlist")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        QzPointContainer(pointid, courseid, pointname, pointyear, chapter, creator, createtime, status, modifystatus, excisenum, pointlistid,
          chapterid, sequence, pointdescribe, pointlevel, typeslist, score, thought, remid, pointnamelist, typelistids,
          pointlist, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point")
  }
  def getQzPointQuestion(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzPointQuestion.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val pointid = jsonObject.getIntValue("pointid")
        val questionid = jsonObject.getIntValue("questionid")
        val questtype = jsonObject.getIntValue("questtype")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (pointid, questionid, questtype, creator, createtime, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point_question")
  }
  def getQzSiteCourse(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzSiteCourse.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val sitecourseid = jsonObject.getIntValue("sitecourseid")
        val siteid = jsonObject.getIntValue("siteid")
        val courseid = jsonObject.getIntValue("courseid")
        val sitecoursename = jsonObject.getString("sitecoursename")
        val coursechapter = jsonObject.getString("coursechapter")
        val sequence = jsonObject.getString("sequence")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val helppaperstatus = jsonObject.getString("helppaperstatus")
        val servertype = jsonObject.getString("servertype")
        val boardid = jsonObject.getIntValue("boardid")
        val showstatus = jsonObject.getString("showstatus")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status, creator
          , createtime, helppaperstatus, servertype, boardid, showstatus, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_site_course")
  }
  def getQzCourseEduSubject(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzCourseEduSubject.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val courseeduid = jsonObject.getIntValue("courseeduid")
        val edusubjectid = jsonObject.getIntValue("edusubjectid")
        val courseid = jsonObject.getIntValue("courseid")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val majorid = jsonObject.getIntValue("majorid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_course_edusubject")
  }
  def getQzCourse(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzCourse.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val courseid = jsonObject.getIntValue("courseid")
        val majorid = jsonObject.getIntValue("majorid")
        val coursename = jsonObject.getString("coursename")
        val coursechapter = jsonObject.getString("coursechapter")
        val sequence = jsonObject.getString("sequnece")
        val isadvc = jsonObject.getString("isadvc")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val status = jsonObject.getString("status")
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        val pointlistid = jsonObject.getIntValue("pointlistid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime, status
          , chapterlistid, pointlistid, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_course")
  }
  def getQzWebSite(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzWebsite.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val domain = jsonObject.getString("domain")
        val sequence = jsonObject.getString("sequence")
        val multicastserver = jsonObject.getString("multicastserver")
        val templateserver = jsonObject.getString("templateserver")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val multicastgateway = jsonObject.getString("multicastgateway")
        val multicastport = jsonObject.getString("multicastport")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime,
          multicastgateway, multicastport, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_website")
  }
  def getQzMajor(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzMajor.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val majorid = jsonObject.getIntValue("majorid")
        val businessid = jsonObject.getIntValue("businessid")
        val siteid = jsonObject.getIntValue("siteid")
        val majorname = jsonObject.getString("majorname")
        val shortname = jsonObject.getString("shortname")
        val status = jsonObject.getString("status")
        val sequence = jsonObject.getString("sequence")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val columm_sitetype = jsonObject.getString("columm_sitetype")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (majorid, businessid, siteid, majorname, shortname, status, sequence, creator, createtime, columm_sitetype, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_major")
  }
  def getQzBusiness(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzBusiness.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val businessid = jsonObject.getIntValue("businessid")
        val businessname = jsonObject.getString("businessname")
        val sequence = jsonObject.getString("sequence")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val siteid = jsonObject.getIntValue("siteid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_business")
  }
  def getQzPaperView(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzPaperView.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val paperid = jsonObject.getIntValue("paperid")
        val paperviewname = jsonObject.getString("paperviewname")
        val paperparam = jsonObject.getString("paperparam")
        val openstatus = jsonObject.getString("openstatus")
        val explainurl = jsonObject.getString("explainurl")
        val iscontest = jsonObject.getString("iscontest")
        val contesttime = jsonObject.getString("contesttime")
        val conteststarttime = jsonObject.getString("conteststarttime")
        val contestendtime = jsonObject.getString("contestendtime")
        val contesttimelimit = jsonObject.getString("contesttimelimit")
        val dayiid = jsonObject.getIntValue("dayiid")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val paperviewcatid = jsonObject.getIntValue("paperviewcatid")
        val modifystatus = jsonObject.getString("modifystatus")
        val description = jsonObject.getString("description")
        val papertype = jsonObject.getString("papertype")
        val downurl = jsonObject.getString("downurl")
        val paperuse = jsonObject.getString("paperuse")
        val paperdifficult = jsonObject.getString("paperdifficult")
        val testreport = jsonObject.getString("testreport")
        val paperuseshow = jsonObject.getString("paperuseshow")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        QzPageViewContainer(paperviewid, paperid, paperviewname, paperparam, openstatus, explainurl, iscontest, contesttime,
          conteststarttime, contestendtime, contesttimelimit, dayiid, status, creator, createtime, paperviewcatid, modifystatus,
          description, papertype, downurl, paperuse, paperdifficult, testreport, paperuseshow, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_paper_view")
  }
  def getQzPaper(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzPaper.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val paperid = jsonObject.getIntValue("paperid")
        val papercatid = jsonObject.getIntValue("papercatid")
        val courseid = jsonObject.getIntValue("courseid")
        val paperyear = jsonObject.getString("paperyear")
        val chapter = jsonObject.getString("chapter")
        val suitnum = jsonObject.getString("suitnum")
        val papername = jsonObject.getString("papername")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val craetetime = jsonObject.getString("createtime")
        val totalscore = BigDecimal.apply(jsonObject.getString("totalscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val chapterid = jsonObject.getIntValue("chapterid")
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator, craetetime, totalscore, chapterid,
          chapterlistid, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_paper")
  }
  def getQzCenterPaper(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzCenterPaper.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val centerid = jsonObject.getIntValue("centerid")
        val openstatus = jsonObject.getString("openstatus")
        val sequence = jsonObject.getString("sequence")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center_paper")
  }

  def getQzCenter(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzCenter.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val centerid = jsonObject.getIntValue("centerid")
        val centername = jsonObject.getString("centername")
        val centeryear = jsonObject.getString("centeryear")
        val centertype = jsonObject.getString("centertype")
        val openstatus = jsonObject.getString("openstatus")
        val centerparam = jsonObject.getString("centerparam")
        val description = jsonObject.getString("description")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val sequence = jsonObject.getString("sequence")
        val provideuser = jsonObject.getString("provideuser")
        val centerviewtype = jsonObject.getString("centerviewtype")
        val stage = jsonObject.getString("stage")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime,
          sequence, provideuser, centerviewtype, stage, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center")
  }
  def getQzQuestionType(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzQuestionType.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val quesviewtype = jsonObject.getIntValue("quesviewtype")
        val viewtypename = jsonObject.getString("viewtypename")
        val questiontypeid = jsonObject.getIntValue("questypeid")
        val description = jsonObject.getString("description")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val papertypename = jsonObject.getString("papertypename")
        val sequence = jsonObject.getString("sequence")
        val remark = jsonObject.getString("remark")
        val splitscoretype = jsonObject.getString("splitscoretype")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (quesviewtype, viewtypename, questiontypeid, description, status, creator, createtime, papertypename, sequence,
          remark, splitscoretype, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_question_type")
  }
  def getQzQuestion(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzQuestion.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val questionid = jsonObject.getIntValue("questionid")
        val parentid = jsonObject.getIntValue("parentid")
        val questypeid = jsonObject.getIntValue("questypeid")
        val quesviewtype = jsonObject.getIntValue("quesviewtype")
        val content = jsonObject.getString("content")
        val answer = jsonObject.getString("answer")
        val analysis = jsonObject.getString("analysis")
        val limitminute = jsonObject.getString("limitminute")
        val score = BigDecimal.apply(jsonObject.getDoubleValue("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val splitscore = BigDecimal.apply(jsonObject.getDoubleValue("splitscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val status = jsonObject.getString("status")
        val optnum = jsonObject.getIntValue("optnum")
        val lecture = jsonObject.getString("lecture")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val modifystatus = jsonObject.getString("modifystatus")
        val attanswer = jsonObject.getString("attanswer")
        val questag = jsonObject.getString("questag")
        val vanalysisaddr = jsonObject.getString("vanalysisaddr")
        val difficulty = jsonObject.getString("difficulty")
        val quesskill = jsonObject.getString("quesskill")
        val vdeoaddr = jsonObject.getString("vdeoaddr")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        QzQuestionContainer(questionid, parentid, questypeid, quesviewtype, content, answer, analysis, limitminute, score, splitscore,
          status, optnum, lecture, creator, createtime, modifystatus, attanswer, questag, vanalysisaddr, difficulty, quesskill,
          vdeoaddr, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_question")
  }
  def getQzMemberPaperQuestion(sc: SparkContext, spark: SparkSession): Unit ={
    import spark.implicits._
    val sourceRdd: RDD[String] = sc.textFile("/user/john/ods/QzMemberPaperQuestion.log")
    sourceRdd.filter{it=>
      val obj: JSONObject = ParseJsonData.getJsonData(it)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions{p=>
      p.map{it=>
        val jsonObject: JSONObject = ParseJsonData.getJsonData(it)
        val userid = jsonObject.getIntValue("userid")
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val chapterid = jsonObject.getIntValue("chapterid")
        val sitecourseid = jsonObject.getIntValue("sitecourseid")
        val questionid = jsonObject.getIntValue("questionid")
        val majorid = jsonObject.getIntValue("majorid")
        val useranswer = jsonObject.getString("useranswer")
        val istrue = jsonObject.getString("istrue")
        val lasttime = jsonObject.getString("lasttime")
        val opertype = jsonObject.getString("opertype")
        val paperid = jsonObject.getIntValue("paperid")
        val spendtime = jsonObject.getIntValue("spendtime")
        val score = BigDecimal.apply(jsonObject.getString("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val question_answer = jsonObject.getIntValue("question_answer")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (userid, paperviewid, chapterid, sitecourseid, questionid, majorid, useranswer, istrue, lasttime, opertype, paperid, spendtime, score, question_answer, dt, dn)
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_member_paper_question")
  }
}




























