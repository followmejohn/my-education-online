package com.john.streaming

import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.john.bean.LearnModel
import com.john.util.{DataSourceUtil, QueryCallback, SqlPorxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

//知识点掌握度实时统计
object QzPointStreaming {
  private val groupid = "qz_point_group"
  private val map = new mutable.HashMap[String,LearnModel]()

  def qzQuestionUpdate(key: String, it: Iterable[(String, String, String, String, String, String)], sqlPorxy: SqlPorxy, client: Connection)= {
    val keys: Array[String] = key.split("-")
    val userid: Int = keys(0).toInt
    val courseid = keys(1).toInt
    val pointid = keys(2).toInt
    val array = it.toArray
    //对当前批次的数据下questionid 去重
    val questionids: Array[String] = array.map(_._4).distinct
    //查询历史数据下的questionid
    var questionids_history: Array[String] = Array()
    sqlPorxy.executeQuery(client, "select questionids from qz_point_history where userid = ? and courseid = ? and pointid=?",
      Array(userid, courseid, pointid),new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()){
            questionids_history = rs.getString(1).split(",")
          }
          rs.close()
        }
      })
    //获取到历史数据后再与当前数据进行拼接 去重
    val resultQuestionid: Array[String] = questionids.union(questionids_history).distinct
    val countSize: Int = resultQuestionid.length//用户做的知识点个数
    val resultQuestionid_str = resultQuestionid.mkString(",")//用户做的知识点用逗号拼接成一个字符串存入历史记录表
    val qz_count = questionids.length//没用到
    var qz_sum = array.length//获取当前批次题总数
    var qz_istrue = array.count(_._5.equals("1"))//获取当前批次做正确的题个数
    val createtime = array.map(_._6).min//获取最早的创建时间 作为表中创建时间
    //更新qz_point_history 历史记录表 此表用于存当前用户做过的questionid表
    val updatetime: String = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
    sqlPorxy.executeUpdate(client,"insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?)"+
    "on duplicate key update questionids=?, updatetime=?", Array(userid, courseid, pointid, resultQuestionid_str,createtime,updatetime,resultQuestionid_str, updatetime))
    var qzSum_history = 0
    var istrue_history = 0
    sqlPorxy.executeQuery(client,"select qz_sum, qz_istrue from qz_point_detail where userid=? and courseid = ? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()){
            qzSum_history += rs.getInt(1)//历史做题总数
            istrue_history += rs.getInt(2)//历史作对题总数
          }
          rs.close()
        }
      })
    qz_sum += qzSum_history//当前批次做题总数+历史做题总数（不去重）
    qz_istrue += istrue_history//当前批次做对题总数+历史做对题总数
    //
    val correct_rate: Double = qz_istrue.toDouble / qz_sum.toDouble
    //
    val qz_detail_rate = countSize.toDouble/ 30
    val mastery_rate = qz_detail_rate * correct_rate
    //
    sqlPorxy.executeUpdate(client,"insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime) "+
    "values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?, qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid,courseid,pointid,qz_sum,countSize,qz_istrue,correct_rate,mastery_rate,createtime,updatetime,qz_sum,countSize,qz_istrue,correct_rate,mastery_rate,updatetime))
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName) //.setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
//      .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(conf,Seconds(3))
//    val sc: SparkContext = ssc.sparkContext
    val topics = Array("qz_log")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      ("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092"),
      ("key.deserializer", classOf[StringDeserializer]),
      ("value.deserializer", classOf[StringDeserializer]),
      ("group.id", groupid),
      ("auto.offset.reset", "earliest"),
      ("enable.auto.commit", "false")
    )
    //查询mysql中是否存在偏移量
    val sqlPorxy = new SqlPorxy
    val offsetMap = new mutable.HashMap[TopicPartition,Long]()
    val client: Connection = DataSourceUtil.getConnection
    try{
      sqlPorxy.executeQuery(client,"select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while(rs.next()){
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset: Long = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()
        }
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }finally{
      sqlPorxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream= if(offsetMap.isEmpty){
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics,kafkaMap))
    }else{
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaMap,offsetMap))
    }
    //过滤不正常的数据， 获取数据
    val dsStream: DStream[(String, String, String, String, String, String)] = stream.filter(it => it.value().split("\t").length == 6)
      .mapPartitions(p => {
        p.map(it => {
          val line: String = it.value()
          val arr: Array[String] = line.split("\t")
          val uid: String = arr(0)
          val courseid = arr(1)
          val pointid = arr(2)
          val questionid = arr(3)
          val istrue = arr(4)
          val createtime = arr(5)
          (uid, courseid, pointid, questionid, istrue, createtime)
        })
      })
    dsStream.foreachRDD(rdd=>{
      //在操控mysql之前先聚合rdd，预防多线程安全问题
      //获取相同用户 同一课程 同一知识点的数据
      val groupRdd: RDD[(String, Iterable[(String, String, String, String, String, String)])] = rdd.groupBy(it=> it._1 + "-" + it._2 + "-" + it._3)
      groupRdd.foreachPartition(p=>{
        //在分区下获取jdbc连接  减少jdbc连接个数
        val sqlPorxy = new SqlPorxy
        val client: Connection = DataSourceUtil.getConnection
        try{
          p.foreach{case (key, it)=> qzQuestionUpdate(key, it, sqlPorxy, client)}//存入mysql
        }catch {
          case e: Exception => e.printStackTrace()
        }finally {
          sqlPorxy.shutdown(client)
        }
      })
    })
    //手动提交offset维护到本地
    stream.foreachRDD(rdd=>{
      val sqlPorxy = new SqlPorxy
      val client: Connection = DataSourceUtil.getConnection
      try{
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for(or <- offsetRanges){
          sqlPorxy.executeUpdate(client, "replace into `offset_manager` (groupid, topic,`partition`, untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      }catch{
        case e: Exception=> e.printStackTrace()
      }finally {
        sqlPorxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}















