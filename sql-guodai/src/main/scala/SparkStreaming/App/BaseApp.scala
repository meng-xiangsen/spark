package SparkStreaming.App

import SparkStreaming.Beans.AdsInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

abstract class BaseApp() {
  val ssc = new StreamingContext("local[*]", "11111", Seconds(3))

  def runApp(op : => Unit)= {
    try {
      op
    } catch {
      case e:Exception =>println(e.getMessage)
    }
    ssc.start()
    ssc.awaitTermination()
  }

  val kafkaParams = Map[String,String](
    "bootstrap.servers" -> "hadoop102:9092",
    "group.id" -> "0722kafka",
    "enable.auto.commit" -> "true",
    "auto.offset.reset" -> "earliest",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  def getDatas()={
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("0722test"), kafkaParams)
    )
    // 将ds中每个ConsumerRecord的value中的值取出，封装为需要的Bean
    val ds1: DStream[AdsInfo] = ds.map(record => {
      // 输入 1604922609982,华东,上海,105,5
      val words = record.value().split(",")

      val bean: AdsInfo = AdsInfo(
        words(0).toLong,
        words(1),
        words(2),
        words(3),
        words(4)
      )
      bean
    })
    ds1
  }




  /*val streamingContext1 = new StreamingContext("local[*]", "wc", Seconds(3))

  // 每个独立的需求，都继承BaseApp，将逻辑通过调用runApp传入
  def runApp(op : => Unit )={

    //用户每个需求的处理逻辑
    try {
      op
    } catch {
      case e:Exception => println(e.getMessage)
    }

    streamingContext1.start()

    streamingContext1.awaitTermination()

  }

  val kafkaParams: Map[String, String] = Map[String, String](
    "bootstrap.servers" -> "hadoop102:9092",
    "group.id" -> "0722kafka",
    "enable.auto.commit" -> "true",
    "auto.offset.reset" -> "earliest",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )
  // 提供公共方法，从Kafka中获取DS

  // 代表从kafka中获取的数据流
  def getDatas()={
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext1,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("0722test"), kafkaParams)
    )
    // 将ds中每个ConsumerRecord的value中的值取出，封装为需要的Bean
    // 1604903340082,华中,杭州,100,2
    val ds1: DStream[AdsInfo] = ds.map(record => {
      val words: Array[String] = record.value().split(",")
      val bean: AdsInfo = AdsInfo(
        words(0).toLong,
        words(1),
        words(2),
        words(3),
        words(4)
      )
      bean
    })

    ds1

  }*/






}
