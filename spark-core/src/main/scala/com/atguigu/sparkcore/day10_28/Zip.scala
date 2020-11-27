package com.atguigu.sparkcore.day10_28

import org.apache.spark.{SparkConf, SparkContext}

object Zip {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dv"))
    val list = List(10,20,30,40,50,60,70)
    val list1 = List(1,2,3,4,5,6,7,8,9)

    val rdd = sc.parallelize(list,3)
    val rdd1 = sc.parallelize(list1,3)
    //rdd.zip(rdd1).collect().foreach(println)

    rdd.zipPartitions(rdd1)((it1,it2)=>{
      //it1.zip(it2)
        //it1.zipAll(it2,100,200)
      it1.zipWithIndex.zip(it2.zipWithIndex)
    }).collect().foreach(println)


    sc.stop()
  }
}
