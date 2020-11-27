package day09

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

import scala.collection.mutable

class SparkStreamingTest {

  @Test
  def testCreateDSFromQueue(): Unit ={
    val queue= mutable.Queue[RDD[String]]()
    val ssc = new StreamingContext("local[*]", "1111", Seconds(3))
    val ds: InputDStream[String] = ssc.queueStream(queue)


  }

  @Test
  def createDStream(): Unit ={

    val conf = new SparkConf().setAppName("1111").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(3))
    println(ssc)
    ssc.sparkContext
    ssc.start()
    ssc.awaitTermination()

  }

  @Test
  def creatDsreaming(): Unit = {
    val ssc = new StreamingContext("local[*]",
      "11111", Seconds(3))

    val lineStreams =
      ssc.socketTextStream("hadoop102", 9999)
    val wordAndCountStreams =
      lineStreams.flatMap(line => line.split(" ")).
        map(word => (word, 1)).reduceByKey(_ + _)

    wordAndCountStreams.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
