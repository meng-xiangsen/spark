package day08

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.{After, Before, Test}


class Exer1 {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")

  private val sparkSession: SparkSession = SparkSession.builder
    .enableHiveSupport().
    config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse").
    config(conf).getOrCreate()

  import sparkSession.implicits._
  @Before
  //删除output目录
  def start(): Unit ={
    val fileSystem = FileSystem.get(new Configuration())
    // 相对于当前project
    // 当前project/output
    val path = new Path("output")
    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
  }
  @After
  def stop(): Unit ={
    sparkSession.close()
  }
  @Test
  def testReadHive() : Unit ={
    //sparkSession.sql("show databases").show()
    //sparkSession.sql("show tables").show()
   /* sparkSession.sql("create database db1")
    sparkSession.sql("use db1")
    sparkSession.sql("create table t1(name string)")
    sparkSession.sql("insert into table t1 values('jack')")*/
    //sparkSession.sql("select * from  db1.t1 ").show()
  }
  @Test
  def testWriteHive() : Unit ={

    val list = List(Person("jack", 20), Person("marry", 21))

    val rdd: RDD[Person] = sparkSession.sparkContext.makeRDD(list, 1)

    val ds: Dataset[Person] = rdd.toDS()

    ds.write.saveAsTable("db1.t2")



  }

}
