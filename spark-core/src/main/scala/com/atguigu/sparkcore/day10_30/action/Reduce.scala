package com.atguigu.sparkcore.day10_30.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Reduce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(10,20,30,40),2)
    println(rdd.reduce(_ + _))
    println(rdd.fold(1)(_ + _))
    val i = rdd.aggregate(1)({
      case (x, y) => x.max(y)
    }, {
      case (x, y) => x + y
    })
    println(i)

    sc.stop()
  }
}
