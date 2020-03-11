package com.john.streaming

import java.sql.{Connection, ResultSet}

import com.john.util.{DataSourceUtil, QueryCallback, SqlPorxy}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.ser.std.StringSerializer

import scala.collection.mutable

object RegisterStreaming {
  private val groupid = "register_group_test"
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("register").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition","100")
    //      .set("spark.streaming.backpressure.enabled", "true")
    //      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //      .setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val sc: SparkContext = ssc.sparkContext
    val topics = Array("register_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      ("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092"),
      ("key.deserializer", classOf[StringSerializer]),
      ("value.deserializer", classOf[StringSerializer]),
      ("group.id", groupid),
      ("auto.offset.reset", "earliest"),
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      ("enable.auto.commit", false)
    )
    //sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("/user/john/sparkstreaming/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlPorxy
    val offsetMap = new mutable.HashMap[TopicPartition,Long]()
    val client: Connection = DataSourceUtil.getConnection
    try{
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid = ?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while(rs.next()){
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset: Long = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()//关闭游标
        }
      })
    }catch{
      case e: Exception => e.printStackTrace()
    }finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream= if(offsetMap.isEmpty){
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics,kafkaMap))
    }else{
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics, kafkaMap, offsetMap))
    }
    val resultDStream= stream.filter(it => it.value().split("\t").length == 3)
      .mapPartitions(p=>{
        p.map(it=>{
          val line: String = it.value()
          val arr: Array[String] = line.split("\t")
          val app_name: String = arr(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (app_name, 1)
        })
      })
    resultDStream.cache()
    //"=================每6s间隔1分钟内的注册数据================="
    resultDStream.reduceByKeyAndWindow((x: Int,y: Int)=> x + y, Seconds(60), Seconds(6)).print
    //"+++++++++++++++++++++++实时注册人数+++++++++++++++++++++++"
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.sum
      val previousCount: Int = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    resultDStream.updateStateByKey(updateFunc).print()
    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd=>{
      val sqlPorxy = new SqlPorxy()
      val client: Connection = DataSourceUtil.getConnection
      try{
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for(or <- offsetRanges){
          sqlPorxy.executeUpdate(client, "replace into `offset_manager` (groupid, topic, `partition`,untilOffset) values(?,?,?,?)",
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
}




















