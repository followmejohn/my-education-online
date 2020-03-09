package com.john.warehouse.qz.service

import com.john.warehouse.qz.dao._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object QzDwsService {
  def dwsQzChapter(spark: SparkSession, dt: String): Unit ={
    val chapter: DataFrame = QzDwsChapterDao.getDwdQzChapter(spark,dt)
    val chapterList: DataFrame = QzDwsChapterDao.getDwdQzChapterList(spark,dt)
    val point: DataFrame = QzDwsChapterDao.getDwdQzPoint(spark,dt)
    val pointQuestion: DataFrame = QzDwsChapterDao.getDwdQzPointQuestion(spark,dt)
    chapter.join(chapterList,Seq("chapterlistid","dn"))
      .join(point,Seq("chapterid","dn"))
      .join(pointQuestion,Seq("pointid","dn"))
      .select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus","showstatus",
        "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
        "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
        "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_chapter")
  }
  def dwsQzCourse(spark: SparkSession, dt: String): Unit ={
    val course: DataFrame = QzDwsCourseDao.getDwdQzCourse(spark,dt)
    val courseEduSubject: DataFrame = QzDwsCourseDao.getDwdQzCourseEduSubject(spark,dt)
    val siteCourse: DataFrame = QzDwsCourseDao.getDwdQzSiteCourse(spark,dt)
    course.join(siteCourse,Seq("courseid","dn"))
      .join(courseEduSubject,Seq("courseid","dn"))
      .select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
        "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
        "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
        , "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_course")
  }
  def dwsQzMajor(spark: SparkSession, dt: String): Unit ={
    val dwdQzMajor = QzDwsMajorDao.getQzMajor(spark, dt)
    val dwdQzWebsite = QzDwsMajorDao.getQzWebsite(spark, dt)
    val dwdQzBusiness = QzDwsMajorDao.getQzBusiness(spark, dt)
    val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
      .join(dwdQzBusiness, Seq("businessid", "dn"))
      .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
        "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
        "multicastgateway", "multicastport", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_major")
  }
  def dwsQzPaper(spark: SparkSession, dt: String): Unit ={
    val dwdQzPaperView = QzDwsPaperDao.getDwdQzPaperView(spark, dt)
    val dwdQzCenterPaper = QzDwsPaperDao.getDwdQzCenterPaper(spark, dt)
    val dwdQzCenter = QzDwsPaperDao.getDwdQzCenter(spark, dt)
    val dwdQzPaper = QzDwsPaperDao.getDwdQzPaper(spark, dt)
    val result = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))
      .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
        , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
        "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
        "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
        "dt", "dn")

    result.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_paper")
  }
  def dwsQzQuestion(spark: SparkSession, dt: String): Unit ={
    val dwdQzQuestion = QzDwsQuestionDao.getQzQuestion(spark, dt)
    val dwdQzQuestionType = QzDwsQuestionDao.getQzQuestionType(spark, dt)
    val result = dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
      .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
        , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
        , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
        "remark", "splitscoretype", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_question")
  }
  def dwsUserPaperDetail(sparkSession: SparkSession, dt: String) = {
    val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt).drop("paperid")
      .withColumnRenamed("question_answer", "user_question_answer")
    val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(sparkSession, dt).drop("courseid")
    val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(sparkSession, dt).withColumnRenamed("sitecourse_creator", "course_creator")
      .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
      .drop("chapterlistid").drop("pointlistid")
    val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
    val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession, dt).drop("courseid")
    val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)
    dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
      join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_user_paper_detail")
  }
}
