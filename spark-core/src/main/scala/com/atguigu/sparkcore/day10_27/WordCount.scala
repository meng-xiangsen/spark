package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("111").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val r = sc.parallelize(List(1, 3, 45, 7))

    println(r.count())
    println(r.first())
    println(r.take(2).mkString)
    println(r.takeOrdered(4)(Ordering.Int.reverse).mkString(","))

    sc.stop()
  }
}
