package com.atguigu.sparkcore.day10_28

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    rdd.groupByKey().collect.foreach(println)

    //(a,CompactBuffer(1, 5))
    //(b,CompactBuffer(5, 2))
    sc.stop()
  }
}
