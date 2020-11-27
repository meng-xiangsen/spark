package com.atguigu.sparkcore.day10_28

import org.apache.spark.{SparkConf, SparkContext}

object DoubleValue {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dv"))
    val list = List(10,20,30,40,50,60)
    val list1 = List(10,20,30,4,5,6)

    val rdd = sc.parallelize(list,2)
    val rdd1 = sc.parallelize(list1,3)


    //rdd.intersection(rdd1).collect().foreach(println(_))

    //rdd.union(rdd1).collect().foreach(print(_))

    rdd.subtract(rdd1).collect().foreach(print(_))

    sc.stop()
  }
}
