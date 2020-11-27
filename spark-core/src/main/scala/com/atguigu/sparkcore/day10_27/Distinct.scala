package com.atguigu.sparkcore.day10_27

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FilterDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20 ,50, 70, 60)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    //rdd1.distinct().collect.foreach(println(_))

    val users = List(User("zhangsan", 21), User("zhangsan", 20))
    val rdd2  = sc.makeRDD(users)
    rdd2.distinct().collect.foreach(println(_))


    sc.stop()
  }
}
case class User(name:String,age:Int){
  override def hashCode(): Int = name.hashCode
  override def equals(obj: Any): Boolean = {
    obj match {
      case User(name1,age1)=> name1 ==this.name
    }
  }
}
