package com.john.streaming

import java.sql.{Connection, ResultSet}

import com.john.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlPorxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



//页面转换率实时统计
object PageStreaming {
  private val groupid = "page_groupid"
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName) //.setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enable", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc= new StreamingContext(conf, Seconds(3))
    val topics = Array("page_topic")
    val kafkaMap = Map[String, Object](
      ("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092"),
      ("key.deserializer",classOf[StringDeserializer])
        ("value.deserializer",classOf[StringDeserializer])
        ("group.id",groupid)
        ("auto.offset.reset","earliest")
        ("enable.auto.commit","false")
    )
      //查询mysql中是否存在偏移量
    val sqlPoxy = new SqlPorxy
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try{
      sqlPoxy.executeQuery(client,"select * from `offset_manager` where groupid = ?",Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()){
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()
        }
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      sqlPoxy.shutdown(client)
    }
    //设置kafka消费数据的参数 判断本地是否有偏移量  有则根据偏移量继续消费 无则从头开始消费
    val stream= if(offsetMap.isEmpty){
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](topics,kafkaMap))
    }else{
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaMap,offsetMap))
    }
    //解析json数据
    val dsStream: DStream[(String, String, String, String, String, String, String)] = stream.map(it => it.value()).mapPartitions(p => {
      p.map(it => {
        val jsonObject = ParseJsonData.getJSONObject(it)
        val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
        val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val page_id = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
        val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        (uid, app_id, device_id, ip, last_page_id, page_id, next_page_id)
      })
    }).filter(it => {
      !it._5.equals("") && !it._6.equals("") && !it._7.equals("")
    })
    dsStream.cache()
    val pageValueDStream = dsStream.map(it=>(it._5 + "_" + it._6 + "_" + it._7,1))
    val resultDStream = pageValueDStream.reduceByKey(_+_)
    resultDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(p=>{
        //在分区下获取jdbc连接
        val sqlPorxy = new SqlPorxy
        val client = DataSourceUtil.getConnection
        try{
          p.foreach(it=>{
            calcPageJumpCount(sqlPorxy, it, client)//计算页面跳转个数
          })
        }catch{
          case e:Exception=> e.printStackTrace()
        }finally {
          sqlPorxy.shutdown(client)
        }
      })
    })
    ssc.sparkContext.addFile("/user/john/sparkstreaming/ip/ip2region.db")//广播文件
    val ipDStream = dsStream.mapPartitions(p=>{
      val dbFile: String = SparkFiles.get("ip2region.db")
      val ipSearch = new DbSearcher(new DbConfig(), dbFile)
      p.map{it=>
        val ip= it._4
        val province: String = ipSearch.memorySearch(ip).getRegion.split("\\|")(2) //获取ip详情   中国|0|上海|上海市|有线通
        (province, 11)
      }
    }).reduceByKey(_+_)
    ipDStream.foreachRDD(rdd=>{
      //查询mysql历史数据 转成rdd
      val ipSqlProxy = new SqlPorxy
      val ipClient: Connection = DataSourceUtil.getConnection
      try{
        val history_data = new ArrayBuffer[(String, Long)]()
        ipSqlProxy.executeQuery(ipClient, "select province, num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()){
              val tuple= (rs.getString(1), rs.getLong(2))
              history_data += tuple
            }
          }
        })
        val history_rdd: RDD[(String, Long)] = ssc.sparkContext.makeRDD(history_data)
        val resultRdd: RDD[(String, Long)] = history_rdd.fullOuterJoin(rdd).map(it=>{
          val province: String = it._1
          val nums: Long = it._2._1.getOrElse(0L) + it._2._2.getOrElse(0L)
          (province, nums)
        })
        resultRdd.foreachPartition(partitions=>{
          val sqlPorxy = new SqlPorxy
          val client: Connection = DataSourceUtil.getConnection
          try{
            partitions.foreach(it=>{
              val province = it._1
              val num = it._2
              //修改mysql数据 并重组返回最新结果数据
              sqlPorxy.executeUpdate(client, "insert into tmp_city_num_detail(province,num) values(?,?) on duplicate key update num =? ",
                Array(province, num, num))
            })
          }catch {
            case e: Exception => e.printStackTrace()
          }finally {
            sqlPorxy.shutdown(client)
          }
        })
        val top3Rdd = resultRdd.sortBy[Long](_._2,false).take(3)
        sqlPoxy.executeUpdate(ipClient, "truncate table top_city_num", null)
        top3Rdd.foreach(it=>{
          sqlPoxy.executeUpdate(ipClient, "insert into top_city_num (province, num) values(?,?)", Array(it._1, it._2))
        })
      }catch{
        case e: Exception => e.printStackTrace()
      }finally {
        sqlPoxy.shutdown(ipClient)
      }
    })
    //计算转换率
    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd =>{
      val sqlPorxy = new SqlPorxy
      val client: Connection = DataSourceUtil.getConnection
      try{
        calcJumpRate(sqlPorxy, client)//计算转换率
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges){
          sqlPorxy.executeUpdate(client, "replace into `offset_manager` (groupid, topic, `partition`, untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      }catch {
        case e: Exception => e.printStackTrace()
      }finally {
        sqlPorxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
  //计算页面跳转个数
  def calcPageJumpCount(sqlPorxy: SqlPorxy, it: (String, Int), client: Connection)= {
    val keys: Array[String] = it._1.split("_")
    var num= it._2.toLong
    val page_id: Int = keys(1).toInt//获取当前page_id
    val last_page_id: Int = keys(0).toInt//获取上一page_id
    val next_page_id: Int = keys(2).toInt//获取下页面page_id
    //查询当前page_id的历史num个数
    sqlPorxy.executeQuery(client, "select num from page_jump_rate where page_id =?",Array(page_id),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()){
          num += rs.getLong(1)
        }
        rs.close()
      }
    })
    //
    if(page_id == 1){//对num 进行修改 并且判断当前page_id是否为首页（商品课程页）
      sqlPorxy.executeUpdate(client, "insert into page_jump_rate(last_page_id, page_id,next_page_id,num,jump_rate)" +
      "values(?,?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, "100%",num))
    }else{
      sqlPorxy.executeUpdate(client,"insert into page_jump_rate(last_page_id, page_id,next_page_id,num)" +
        "values(?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num,num))
    }
  }
  //计算转换率
  def calcJumpRate(sqlPorxy: SqlPorxy, client: Connection)={
    var page1_num = 0L
    var page2_num = 0L
    var page3_num = 0L
    sqlPorxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(1),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()){
          page1_num = rs.getLong(1)
        }
      }
    })
    sqlPorxy.executeQuery(client,"select num from page_jump_rate where page_id=?",Array(2),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()){
          page2_num = rs.getLong(1)
        }
      }
    })
    sqlPorxy.executeQuery(client,"select num from page_jump_rate where page_id=?",Array(3),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          page3_num = rs.getLong(1)
        }
      }
    })
  }
}























