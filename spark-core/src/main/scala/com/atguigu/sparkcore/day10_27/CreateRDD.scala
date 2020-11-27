package com.atguigu.sparkcore.day10_27

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("RDD")
    val sc = new SparkContext(conf)
    val list = List(30, 50, 70, 60, 10, 20)
    val rdd: RDD[Int] = sc.parallelize(list)
    //val rdd = sc.makeRDD(list)
    rdd.collect().foreach(println(_))
    sc.stop()
  }
}
