package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}

object Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Map")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd = sc.parallelize(list1)
    rdd.glom().collect.foreach(arr => println(arr.mkString("-")))
    sc.stop()
  }
}
