package com.john.warehouse.member.dao

import org.apache.spark.sql.SparkSession

object AdsMemberDao {
  def getMemberData(spark: SparkSession)={
    spark.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
      "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ")
  }
}
