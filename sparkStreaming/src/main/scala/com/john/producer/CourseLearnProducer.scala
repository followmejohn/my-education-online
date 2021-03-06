package com.john.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CourseLearnProducer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("course").setMaster("local[*]")
    val ssc = new SparkContext(conf)
    val rdd: RDD[String] = ssc.textFile("/user/john/ods/course_learn.log",10)
    rdd.foreachPartition(p=>{
      val prop = new Properties()
      prop.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
      prop.put("acks","1")
      prop.put("batch.size","16384")//batch.size是producer批量发送的基本单位，默认是16384Bytes，即16kB；
      prop.put("linger.ms","10")//lingger.ms是sender线程在检查batch是否ready时候，判断有没有过期的参数，默认大小是0ms。
      prop.put("buffer.memory","33554432")
      prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
      prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String,String](prop)
      p.foreach(it=>{
        val msg = new ProducerRecord[String,String]("course_learn",it)
        producer.send(msg)
      })
      producer.flush()
      producer.close()
    })
  }
}
