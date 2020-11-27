package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}

object $02MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Map")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)

    val rdd = sc.parallelize(list1)
  rdd.mapPartitions(it=>{

      it.map(x=>x)

    }).collect().foreach(println(_))

    /*rdd.mapPartitionsWithIndex((index, it) => {
      val tuples = it.map(x => (index, x))
      tuples
    }).collect.foreach(println(_))*/

    sc.stop()
  }
}
