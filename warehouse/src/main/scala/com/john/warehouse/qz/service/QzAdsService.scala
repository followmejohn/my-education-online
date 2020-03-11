package com.john.warehouse.qz.service

import com.john.warehouse.qz.dao.QzAdsDao
import org.apache.spark.sql.{SaveMode, SparkSession}

object QzAdsService {
  def getResult(spark: SparkSession, dt: String): Unit ={
    QzAdsDao.avgScoreAndTime(spark,dt).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_avgtimeandscore")
    QzAdsDao.paparTop3(spark,dt).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_top3_userdetail")
    QzAdsDao.paperLow3(spark,dt).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_low3_userdetail")
    QzAdsDao.paperPassOrNo(spark,dt).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_paper_detail")
    QzAdsDao.scoreHighAndLow(spark,dt).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_maxdetail")
    QzAdsDao.stageDistribution(spark,dt).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_scoresegment_user")
    QzAdsDao.questionCount(spark,dt).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_question_detail")
  }
}
