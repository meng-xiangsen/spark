package com.atguigu.sparkcore.day10_28

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggreegateByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("b", 4), ("b", 3), ("a", 6), ("a", 8)), 2)


    //rdd.aggregateByKey(Int.MinValue)((x,y)=>math.max(x,y),(_+_)).collect.foreach(println)

    val rdd2 = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))({
      case ((max, min), v) => (math.max(max, v), math.min(min, v))
    },
      {
        case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
      }
    )
rdd2.collect.foreach(println)



    sc.stop()
  }
}
