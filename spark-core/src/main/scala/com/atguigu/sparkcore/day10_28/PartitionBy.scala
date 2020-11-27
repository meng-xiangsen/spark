package com.atguigu.sparkcore.day10_28

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object PartitionBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)

    val rdd1 = rdd.partitionBy(new MyPartitioner(3))
    val rdd2 = rdd1.partitionBy(new MyPartitioner(3))
    rdd2.mapPartitionsWithIndex((index,it)=>{
      it.map(it=>(index,it))
    }).collect.foreach(println)


    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}

class MyPartitioner(val num:Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = 0

  override def equals(obj: Any): Boolean = {
        obj match {
          case other:MyPartitioner => this.num == other.num
          case _ => false
        }
  }

  override def hashCode(): Int = num
}
