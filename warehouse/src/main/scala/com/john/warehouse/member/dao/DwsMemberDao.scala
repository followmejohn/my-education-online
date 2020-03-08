package com.john.warehouse.member.dao

import org.apache.spark.sql.SparkSession

object DwsMemberDao {
  def getDwsMember(spark: SparkSession)={
    spark.sql("select uid,ad_id,email,fullname,iconurl,lastlogin,mailaddr,memberlevel," +
      "password,phone,qq,register,regupdatetime,unitname,userip,zipcode,dt,dn from dwd.dwd_member")
  }
  def getDwsMemberRegType(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,appkey,appregurl,bdp_uuid,createtime as reg_createtime,domain,isranreg," +
      "regsource,regsourcename,websiteid as siteid,dn from dwd.dwd_member_regtype ")
  }

  def getDwsBaseAd(sparkSession: SparkSession) = {
    sparkSession.sql("select adid as ad_id,adname,dn from dwd.dwd_base_ad")
  }

  def getDwsBaseWebSite(sparkSession: SparkSession) = {
    sparkSession.sql("select siteid,sitename,siteurl,delete as site_delete," +
      "createtime as site_createtime,creator as site_creator,dn from dwd.dwd_base_website")
  }

  def getDwsVipLevel(sparkSession: SparkSession) = {
    sparkSession.sql("select vip_id,vip_level,start_time as vip_start_time,end_time as vip_end_time," +
      "last_modify_time as vip_last_modify_time,max_free as vip_max_free,min_free as vip_min_free," +
      "next_level as vip_next_level,operator as vip_operator,dn from dwd.dwd_vip_level")
  }

  def getDwsPcentermemPayMoney(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,cast(paymoney as decimal(10,4)) as paymoney,vip_id,dn from dwd.dwd_pcentermempaymoney")
  }
}
