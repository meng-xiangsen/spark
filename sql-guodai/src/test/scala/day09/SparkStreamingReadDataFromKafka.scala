package day09

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

class SparkStreamingReadDataFromKafka {
  @Test
  def ConsumerDataFromKafkaDirect(): Unit = {
    val ssc = new StreamingContext("local[*]", "wc", Seconds(3))
    // 构建消费者参数的Map集合  参考：ConsumerConfig
    val kafkaParams = Map[String, String](

      "bootstrap.servers" -> "hadoop102:9092",
      "enable.auto.commit" -> "true",
      //"auto.commit.interval.ms" -> ""
      "auto.offset.reset" -> "earliest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "0722kafka"
    )
    // 代表从kafka中获取的数据流
    val ds = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("0722"), kafkaParams)
    )

    ds.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 应用程序处理逻辑
      val rdd1 = rdd.map(record => {

        record.value()
      })
      rdd1.foreach(value => println(value))
      // 应用程序处理完成后，手动提交offset
      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }


}
