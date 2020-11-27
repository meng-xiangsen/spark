package day08



import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.{After, Before, Test}

class SparkSqlReadAndWriteTest {

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")

  private val sparkSession: SparkSession = SparkSession.builder.
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
  def testReadJDBC()={
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","123456")
    val df = sparkSession.read.jdbc("jdbc:mysql://hadoop102:3306/demo", "user", properties)
    df.createTempView("t1")
    sparkSession.sql("select * from t1").show()

  }
  @Test
  def testWriteJDBC() : Unit ={

    val list = List(Person("jack", 20), Person("marry", 21))

    val rdd: RDD[Person] = sparkSession.sparkContext.makeRDD(list, 1)

    val properties = new Properties()

    properties.put("user","root")
    properties.put("password","123456")

    val ds: Dataset[Person] = rdd.toDS()

    // ①在mysql中提前建表，表的结构需要和DS的结构匹配(字段数量，类型)  ②让sparksql帮助建表
    ds.write.jdbc( "jdbc:mysql://hadoop102:3306/demo","person",properties)

  }




}
case class Person(name:String,age:Int){

}