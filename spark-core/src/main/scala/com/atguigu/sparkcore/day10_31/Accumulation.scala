package com.atguigu.sparkcore.day10_31

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object Accumulation {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[1]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    //3.1 打印单词出现的次数（a,10） 代码执行了shuffle，效率比较低
    dataRDD.reduceByKey(_ + _).collect().foreach(println)

    //3.2 如果不用shuffle，怎么处理呢？
    var sum = 0
    // 打印是在Executor端
    dataRDD.foreach {
      case (a, count) => {
        sum = sum + count
        println("sum=" + sum)
      }
    }
    // 打印是在Driver端
    println(("a", sum))
    val sum1: LongAccumulator = sc.longAccumulator("sum1")
    dataRDD.foreach{
      case (x,count) => {
        sum1.add(count)
        println(sum1.value)
      }
    }
    println(sum1.value)

    val value: RDD[(String, Int)] = dataRDD.map(t => {
      //3.2 累加器添加数据
      sum1.add(1)
      t
    })

    //3.3 调用两次行动算子，map执行两次，导致最终累加器的值翻倍
    value.foreach(println)
    value.collect()

    //3.4 获取累加器的值
    println("a:"+sum1.value)





    sc.stop()
  }

}
