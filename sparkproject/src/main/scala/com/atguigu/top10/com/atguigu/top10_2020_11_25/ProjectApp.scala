package com.atguigu.top10.com.atguigu.top10_2020_11_25

import com.atguigu.top10.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("11111")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\尚硅谷\\016spark_最新\\03_spark_core数据/user_visit_action.txt")

    val actionRDD = rdd.map(line => {
      val datas = line.split("_")
      UserVisitAction(
        datas(0),
        datas(1).toLong,
        datas(2),
        datas(3).toLong,
        datas(4),
        datas(5),
        datas(6).toLong,
        datas(7).toLong,
        datas(8),
        datas(9),
        datas(10),
        datas(11),
        datas(12).toLong
      )
    })



    sc.stop()
  }
}
