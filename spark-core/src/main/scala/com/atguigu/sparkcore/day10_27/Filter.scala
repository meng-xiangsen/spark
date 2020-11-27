package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FilterDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)

    //rdd1.filter(x=> x>=60 ).collect.foreach(println(_))

    /*rdd1.flatMap(x=> {
      if(x>=60) List(x) else Nil
    }).collect.foreach(println(_))*/

    rdd1.mapPartitions(it =>{
      it.filter(x=> x>=60)
    }).collect.foreach(println(_))


    sc.stop()
  }
}
