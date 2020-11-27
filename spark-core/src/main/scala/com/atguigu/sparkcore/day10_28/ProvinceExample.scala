package com.atguigu.sparkcore.day10_28

import org.apache.spark.{SparkConf, SparkContext}

object ProvinceExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProvinceExample").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("C:\\Users\\孟\\Desktop/agent.log")

    val rdd1 = rdd.map(line => {
      val strings = line.split(" ")
      val pro = strings(1)
      val ad = strings(4)
      ((pro, ad), 1)
    })
    val rdd2 = rdd1.reduceByKey((x, y) => x + y)
    val rdd3 = rdd2.map {
      case ((pro, ad), count) => (pro, (ad, count))
    }
    val rdd4 = rdd3.groupByKey()

    rdd4.mapValues(x=>{
      x.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    }).collect.foreach(println)









    // 时间戳，省份，城市，用户，广告
    // （省份，广告, 100）,(省份，广告, 99）,(省份，广告, 98）
    // （省份，（省份，广告, 100）,(省份，广告, 99）,(省份，广告, 98））
    // （省份，（省份，广告, 1）,(省份，广告, 99）,(省份，广告, 9）。。。）
    sc.stop()

  }
}
