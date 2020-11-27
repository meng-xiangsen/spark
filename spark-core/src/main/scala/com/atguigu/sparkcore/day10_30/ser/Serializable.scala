package com.atguigu.sparkcore.day10_30.ser

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Serializable {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建两个对象
    val user1 = new User("zhangsan")

    val user2 = new User("lisi")

    val users = List(user1, user2)

    val rdd = sc.parallelize(users, 2)
    /*rdd.collect
      .foreach(println)*/
    val userRDD2: RDD[User] = sc.makeRDD(List())
    //userRDD2.foreach(user => println(user.name))
    userRDD2.foreach(user => println(user1.name))
    sc.stop()

  }
}
class User(var name:String)
