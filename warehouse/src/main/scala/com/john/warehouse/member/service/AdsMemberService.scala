package com.john.warehouse.member.service

import com.john.warehouse.bean.QueryResult
import com.john.warehouse.member.dao.AdsMemberDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object AdsMemberService {
  def countByUrl(spark: SparkSession, dt: String)={
    import spark.implicits._
    val sourceDs: Dataset[QueryResult] = AdsMemberDao.getMemberData(spark).as[QueryResult].where(s"dt=${dt}")
    sourceDs.cache()
    //统计注册来源url人数
    sourceDs.mapPartitions(p => p.map(it => (it.appregurl + it.dn + it.dt, 1)))
      .groupByKey(_._1)
      .mapValues(it => it._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val appregurl = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (appregurl, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")
      //各所属网站（sitename）的用户数
    sourceDs.mapPartitions(partiton => {
      partiton.map(item => (item.sitename + item.dn +  item.dt, 1))
    }).groupByKey(_._1)
      .mapValues(item => item._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val sitename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (sitename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_sitenamenum")
    //统计所属来源人数 pc mobile wechat app
    sourceDs.mapPartitions(partition => {
      partition.map(item => (item.regsourcename + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (regsourcename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_regsourcenamenum")

    //统计通过各广告进来的人数
    sourceDs.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val adname = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (adname, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_adnamenum")

    //统计各用户等级人数
    sourceDs.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val memberlevel = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (memberlevel, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_memberlevelnum")

    //统计各用户vip等级人数
    sourceDs.mapPartitions(partition => {
      partition.map(item => (item.vip_level + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val vip_level = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (vip_level, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_viplevelnum")
    //统计各memberlevel等级 支付金额前三的用户
    import org.apache.spark.sql.functions._
    sourceDs.withColumn("rownum",row_number().over(Window.partitionBy("dn","memberlevel").orderBy(desc("paymoney"))))
        .where("rownum < 4").orderBy("memberlevel","rownum")
      .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname" , "sitename", "vip_level", "paymoney", "rownum", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")

  }
}
