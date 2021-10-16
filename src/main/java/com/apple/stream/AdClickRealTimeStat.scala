package com.apple.stream

import com.apple.commons.conf.ConfigurationManager
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Program: commerce
  * @ClassName: AdClickRealTimeStat
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-15 10:16
  * @Version 1.1.0
  **/

/**
  * 日志格式：
  * timestamp province city userid adid
  * 某个时间点 某个省份 某个城市 某个用户 某个广告
  */
object AdClickRealTimeStat {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AdClickRealTimeStat")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    //设置检查点
    ssc.checkpoint("streaming_checkpoint")

    //获取Kafka配置
    val broker_list = ConfigurationManager.config.getString("kafka.broker.list");
    val topics = ConfigurationManager.config.getString("kafka.topics")

    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "commerce-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /**
      * 创建DStream，返回接收到的输入数据
      * LocationStrategies：根据给定的主题和集群地址创建consumer
      *  LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
      * ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
      *  ConsumerStrategies.Subscribe：订阅一系列主题
      */
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam)
    )

    var adRealTimeValueDStream = adRealTimeLogDStream.map(consumerRecordRDD => consumerRecordRDD.value())

    // 用于Kafka Stream的线程非安全问题，重新分区切断血统
    adRealTimeValueDStream = adRealTimeValueDStream.repartition(400)

    //timestamp province city userid adid
    //根据动态黑名单进行数据过滤(userid, timestamp province city userid adid)

    fiterByBlacklist(spark, adRealTimeValueDStream)

  }

  /**
    * 根据黑名单进行过滤
    *
    * @param spark
    * @param adRealTimeValueDStream
    */
  def fiterByBlacklist(spark: SparkSession, adRealTimeValueDStream: DStream[String]): Unit = {

    //    val filteredAdRealTimeLogDStream = adRealTimeValueDStream.transform { consumerRecordRDD =>
    //
    //
    //    }
    // filteredAdRealTimeLogDStream
  }
}
