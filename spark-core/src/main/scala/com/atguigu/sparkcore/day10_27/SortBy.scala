package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SortBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FilterDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20 )
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)

    //rdd1.sortBy(x=> -x).collect.foreach(println(_))
    //rdd1.sortBy((x=> x),ascending = false).collect.foreach(println(_))
    val list2 = List("abc", "aa", "ccc", "abcd", "zzz")
    //println(list2.sortBy(x => x.length)(Ordering.Int))
    //list2.sortBy(x=>(x.length,x)(Ordering.Tuple2(Ordering.Int,Ordering.String.reverse))
    //  list2.sortBy(s => (s.length, s))(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse))



    val rdd2 = sc.parallelize(list2)
    rdd2.sortBy(x=>(x.length,x),ascending = true)(Ordering.Tuple2(Ordering.Int,Ordering.String.reverse),implicitly[ClassTag[(Int, String)]]).collect.foreach(println(_))

    //rdd2.sortBy(s => (s.length, s), true)(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse), implicitly[ClassTag[(Int, String)]]).collect()

    sc.stop()
  }
}
