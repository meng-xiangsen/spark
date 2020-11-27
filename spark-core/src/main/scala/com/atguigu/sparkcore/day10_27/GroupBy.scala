package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(3, 50, 70, 6, 1, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)

    rdd1.groupBy(x=>{
      if(x%2==0)"oushu" else "jishu"
    }).collect.foreach(println)

    val rdd2 = sc.textFile("D:\\mdTest\\hello.txt")
    val value: RDD[(String, Iterable[String])] = rdd2.flatMap(x => x.split(" ")).groupBy(x => x)

    value.map(x=>(x._1,x._2.size)).collect.foreach(println)
   /* value.map(x=>{
      x match {
        case (word,wordList) => (word,wordList.size)
      }
    })*/
    value.map({
      case (word,wordList) => (word,wordList.size)
    })




    sc.stop()
  }
}
