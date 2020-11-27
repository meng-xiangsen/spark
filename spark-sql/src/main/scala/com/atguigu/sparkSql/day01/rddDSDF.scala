package com.atguigu.sparkSql.day01

import org.apache.spark.sql.{DataFrame, SparkSession}

object rddDSDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("1111111")
      .getOrCreate()
    import spark.implicits._

    val rdd = spark.sparkContext.makeRDD(List(Emp("ls", 40), Emp("zs", 30), Emp("ww", 50)))
    val rdd1 = spark.sparkContext.makeRDD(List(("ls", 40), ("zs", 30), ("ww", 50)))

    rdd1.map{
      case(x)=>Emp(x._1,x._2)
    }.toDS().show()

    val frame = rdd1.map({
      case (name, age) => Emp(name, age)
    }).toDF().as[Emp]


    spark.stop()
  }
}
case class Emp(name:String,age:Long)