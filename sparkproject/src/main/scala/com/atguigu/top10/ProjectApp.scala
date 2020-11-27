package com.atguigu.top10

import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Top10")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\尚硅谷\\016spark_最新\\03_spark_core数据/user_visit_action.txt")
    val actionRdd = rdd.map(line => {
      // 获取一行数据
      val datas = line.split("_")
      // 将解析的数据封装到 UserVisitAction
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

    //val top10 = Top10.getTop10(sc, actionRdd)
   //CategoryTopSesseion.statTop10Session_4(sc, actionRdd, top10)
Top10.getTop10_2(sc, actionRdd)

    sc.stop()
  }
}
