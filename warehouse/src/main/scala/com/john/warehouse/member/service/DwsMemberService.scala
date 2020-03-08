package com.john.warehouse.member.service

import com.john.warehouse.bean.{DwsMember, DwsMemberResult, MemberZipper, MemberZipperResult}
import com.john.warehouse.member.dao.DwsMemberDao
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DwsMemberService {
  def importMember(spark: SparkSession, dt: String): Unit ={
    import spark.implicits._
    val dwsMember: DataFrame = DwsMemberDao.getDwsMember(spark).where(s"dt='${dt}'")
    val dwsMemberRegtype = DwsMemberDao.getDwsMemberRegType(spark)
    val dwsBaseAd = DwsMemberDao.getDwsBaseAd(spark)
    val dwsBaseWebsite = DwsMemberDao.getDwsBaseWebSite(spark)
    val dwsPcentermemPaymoney = DwsMemberDao.getDwsPcentermemPayMoney(spark)
    val dwsVipLevel = DwsMemberDao.getDwsVipLevel(spark)
    val dwsMemberDs: Dataset[DwsMember] = dwsMember.join(dwsMemberRegtype, Seq("uid", "dn"), "left")
      .join(dwsBaseAd, Seq("ad_id", "dn"), "left_outer")
      .join(dwsBaseWebsite, Seq("siteid", "dn"), "left_outer")
      .join(dwsPcentermemPaymoney, Seq("uid", "dn"), "left_outer")
      .join(dwsVipLevel, Seq("vip_id", "dn"), "left_outer")
      .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
        , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
        , "appregurl", "bdp_uuid", "reg_createtime", "domain", "isranreg", "regsource", "regsourcename", "adname"
        , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
        "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
        , "vip_operator", "dt", "dn").as[DwsMember]
    val resultData = dwsMemberDs.groupByKey(item => item.uid + "_" + item.dn)
      .mapGroups { case (key, iters) =>
        val keys = key.split("_")
        val uid = Integer.parseInt(keys(0))
        val dn = keys(1)
        val dwsMembers = iters.toList
        val paymoney = dwsMembers.filter(_.paymoney != null).map(_.paymoney).reduceOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString
        val ad_id = dwsMembers.map(_.ad_id).head
        val fullname = dwsMembers.map(_.fullname).head
        val icounurl = dwsMembers.map(_.iconurl).head
        val lastlogin = dwsMembers.map(_.lastlogin).head
        val mailaddr = dwsMembers.map(_.mailaddr).head
        val memberlevel = dwsMembers.map(_.memberlevel).head
        val password = dwsMembers.map(_.password).head
        val phone = dwsMembers.map(_.phone).head
        val qq = dwsMembers.map(_.qq).head
        val register = dwsMembers.map(_.register).head
        val regupdatetime = dwsMembers.map(_.regupdatetime).head
        val unitname = dwsMembers.map(_.unitname).head
        val userip = dwsMembers.map(_.userip).head
        val zipcode = dwsMembers.map(_.zipcode).head
        val appkey = dwsMembers.map(_.appkey).head
        val appregurl = dwsMembers.map(_.appregurl).head
        val bdp_uuid = dwsMembers.map(_.bdp_uuid).head
        val reg_createtime = dwsMembers.map(_.reg_createtime).head
        val domain = dwsMembers.map(_.domain).head
        val isranreg = dwsMembers.map(_.isranreg).head
        val regsource = dwsMembers.map(_.regsource).head
        val regsourcename = dwsMembers.map(_.regsourcename).head
        val adname = dwsMembers.map(_.adname).head
        val siteid = dwsMembers.map(_.siteid).head
        val sitename = dwsMembers.map(_.sitename).head
        val siteurl = dwsMembers.map(_.siteurl).head
        val site_delete = dwsMembers.map(_.site_delete).head
        val site_createtime = dwsMembers.map(_.site_createtime).head
        val site_creator = dwsMembers.map(_.site_creator).head
        val vip_id = dwsMembers.map(_.vip_id).head
        val vip_level = dwsMembers.map(_.vip_level).max
        val vip_start_time = dwsMembers.map(_.vip_start_time).min
        val vip_end_time = dwsMembers.map(_.vip_end_time).max
        val vip_last_modify_time = dwsMembers.map(_.vip_last_modify_time).max
        val vip_max_free = dwsMembers.map(_.vip_max_free).head
        val vip_min_free = dwsMembers.map(_.vip_min_free).head
        val vip_next_level = dwsMembers.map(_.vip_next_level).head
        val vip_operator = dwsMembers.map(_.vip_operator).head
        DwsMemberResult(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
          phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
          bdp_uuid, reg_createtime, domain, isranreg, regsource, regsourcename, adname, siteid,
          sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
          vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
          vip_next_level, vip_operator, dt, dn)
      }
    resultData.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")
    //查询当天增量数据
//    val today: Dataset[MemberZipper] = spark.sql(
//      s"""
//select
//   a.uid,
//   sum(cast(paymoney as decimal(10,4))) as paymoney,
//   max(b.vip_level) as vip_level,
//   from_unixtime(unix_timestamp('${dt}','yyyyMMdd'),'yyyy-MM-dd) as start_time,
//   '9999-12-12' as end_time,
//   firsh(a.dn) as dn
//from dwd.dwd_pcentermempaymoney a
//   join dwd.dwd_vip_level b
//   on a.vip_id = b.vip_id and a.dn = b.dn
//where a.dt='${dt}'
//        group by uid
//      """.stripMargin).as[MemberZipper]
//    //历史数据
//    val history: Dataset[MemberZipper] = spark.sql("select * from dws.dws_member_zipper").as[MemberZipper]
//    //结合
//    today.union(history).groupByKey(it=>it.uid + "_" + it.dn)
//      .mapGroups{case(key,iter)=>
//        val arr: Array[String] = key.split("_")
////        val uid: String = arr(0)
////          val dn: String = arr(1)
//          val zippers: List[MemberZipper] = iter.toList.sortBy(it=>it.start_time)
//          if(zippers.size > 1 && "9999-12-12"== zippers.last.end_time){
//            zippers(zippers.size - 2).end_time = zippers.last.start_time
//            zippers.last.end_time = "9999-12-12"
//            zippers.last.paymoney = BigDecimal.apply(zippers.last.paymoney) + BigDecimal.apply(zippers(zippers.size - 2).paymoney) + ""
//          }
//          MemberZipperResult(zippers)
//      }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")
    //查询当天增量数据
    val dayResult = spark.sql(s"""
                                        |select
                                        |    a.uid,
                                        |    sum(cast(a.paymoney as decimal(10,4))) as paymoney,
                                        |    max(b.vip_level) as vip_level,
                                        |    from_unixtime(unix_timestamp('${dt}','yyyyMMdd'),'yyyy-MM-dd') as start_time,
                                        |    '9999-12-31' as end_time,
                                        |    first(a.dn) as dn
                                        |from
                                        |    dwd.dwd_pcentermempaymoney a
                                        |    join dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn
                                        |where a.dt='${dt}'
                                        |group by uid""".stripMargin).as[MemberZipper]
    //查询历史拉链表数据
    val historyResult = spark.sql("select * from dws.dws_member_zipper").as[MemberZipper]
    //两份数据根据用户id进行聚合 对end_time进行重新修改
    val reuslt = dayResult.union(historyResult)
      .groupByKey(item => item.uid + "_" + item.dn)
      //(MemberZipper(1001,100,银卡,2019-01-01,2019-01-02),MemberZipper(1001,300,银卡,2019-01-02,2019-01-03))... 变为了下面的数据形式
      //(1001_webA,MemberZipper(1001,100,银卡,2019-01-01,2019-01-02),MemberZipper(1001,300,银卡,2019-01-02,2019-01-03))
      // ,(1002_webA,MemberZipper(1002,100,金卡...),MemberZipper(1002,200,金卡...))
      .mapGroups {
      case (key, iters) =>
        //val keys = key.split("_")
        //val uid = keys(0)
        //val dn = keys(1)
        val list = iters.toList.sortBy(item => item.start_time) //对开始时间进行排序
        if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
          //如果存在历史数据 需要对历史数据的end_time进行修改
          /*
          1001,100,银卡,20190101,20190102,webA
          1001,200,银卡,20190102,20190103,webA
          1001,250,银卡,20190103,99991231,webA
           */
          //获取历史数据的最后一条数据
          val oldLastModel = list(list.size - 2)
          //获取当前时间最后一条数据
          val lastModel = list(list.size - 1)
          oldLastModel.end_time = lastModel.start_time
          lastModel.paymoney = (BigDecimal.apply(lastModel.paymoney) + BigDecimal(oldLastModel.paymoney)).toString()
        }
        MemberZipperResult(list)
    }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper") //重组对象打散 刷新拉链表
  }
}
