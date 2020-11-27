package com.atguigu.sparkSql.day01

import org.apache.spark.sql.{DataFrame, SparkSession}

object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("22222")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("datas/employees.json")

    df.createOrReplaceTempView("emp")
    spark.udf.register("addName",(x:String)=>"Name:"+x)

    spark.sql("select addName(name),salary from emp").show




    spark.stop()
  }
}
