package com.atguigu.sparkcore.day10_28

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object CombineByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 2), ("a", 7), ("a", 5), ("b", 2)),2)

    val rdd1: RDD[(String, (Int,Int))] = rdd.combineByKey(
      (v: Int) => (v, 1),
      {
        case ((sum, count), v) =>
          (sum + v, count + 1)
      },
      {
        case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
      }
    )
    val rdd2 = rdd1.mapValues({
      case (sum, count) => sum.toDouble / count
    })
    rdd2.collect.foreach(println)


    sc.stop()
  }
}
