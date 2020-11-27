package com.atguigu.sparkcore.day10_31

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MyAcc {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"), 2)




    sc.stop()
  }
}
class MyAccu extends AccumulatorV2[String, mutable.Map[String, Long]]{
  // 定义输出数据集合
  private val map: mutable.Map[String, Long] = mutable.Map[String, Long]()
  // 是否为初始化状态，如果集合数据为空，即为初始化状态
  override def isZero: Boolean = map.isEmpty
  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    val acc = new MyAccu
    acc.map ++= this.map
    acc
  }
  // 重置累加器
  override def reset(): Unit = map.clear()
  // 增加数据
  override def add(v: String): Unit = {

      // 业务逻辑
      if (v.startsWith("H")) {
        map(v) = map.getOrElse(v, 0L) + 1L
      }

  }
  // 合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    other.value.foreach{
      case (x,count) =>{
        map(x)=map.getOrElse(x,0l)+count
      }
    }
  }

  override def value: mutable.Map[String, Long] = map
}
