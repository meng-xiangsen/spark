package com.atguigu.sparkSql.day01

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("22222")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("datas/employees.json")

    df.createOrReplaceTempView("emp")
    spark.udf.register("myAvg",functions.udaf(new MyAvgUDAF()))

spark.sql("select myAvg(salary) from emp").show




    spark.stop()
  }

class MyAvgUDAF extends Aggregator[Long,Buff,Double]{

  override def zero: Buff = Buff(0L,0L)

  override def reduce(buff: Buff, salary: Long): Buff = {
    buff.sum += salary
    buff.count += 1
    buff
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: Buff): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
  case class Buff(var sum:Long,var count:Long)

}


