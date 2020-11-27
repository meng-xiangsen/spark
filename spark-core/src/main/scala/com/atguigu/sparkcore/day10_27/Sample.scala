package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FilterDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20 )
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)

    rdd1.sample(false,0.5).collect.foreach(println(_))

    sc.stop()
  }
}
