package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkContext对象
    //val conf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val conf = new SparkConf().setAppName("wordcount")

    val sc = new SparkContext(conf)
    // 2. 获取一个RDD
    val lineRDD = sc.textFile(args(0))
    // 3. 对RDD做各种转换
    lineRDD.flatMap(x=>x.split(" ")).map((_,1)).reduceByKey(_+_)
    // 4. 有一个行动算子
      .collect().foreach(println(_))
    // 5. 关闭SparkContext
    sc.stop()
  }
}
