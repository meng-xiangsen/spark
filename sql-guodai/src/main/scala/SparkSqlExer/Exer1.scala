package SparkSqlExer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object  Exer1 {
  private val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("My app").
    // 让spark读取hive-site.xml配置文件，连接指定的hive，而不是使用内置自带的hive
    enableHiveSupport().
    config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse").
    getOrCreate()

  def main(args: Array[String]) = {

    sparkSession.sql("use db1")
    var mybuf = new MyUdaf

    sparkSession.udf.register("mybuf", functions.udaf(mybuf))

    val sql1 =
      """
        |select ci.*,pi.product_name,pi.product_id
        |from (select city_id,click_product_id from user_visit_action where click_product_id != -1) t1
        |left join city_info ci on t1.city_id = ci.city_id
        |left join product_info pi on t1.click_product_id = pi.product_id;
        |""".stripMargin

    val df1: DataFrame = sparkSession.sql(sql1)
    df1.createTempView("t2")

    val sql2 =
      """
        |SELECT
        |	area,product_name,count(*) clickcount ,mybuf(city_name) cityinfo
        |from t2
        |group by area,product_id,product_name
        |""".stripMargin

    val df2: DataFrame = sparkSession.sql(sql2)
    df2.createTempView("t3")

    val sql3 =
      """
        |SELECT
        |	area,product_name,clickcount,cityinfo,
        |	RANK() over(PARTITION by area order by clickcount desc) rn
        |from t3
        |""".stripMargin
    val df3: DataFrame = sparkSession.sql(sql3)
    df3.createTempView("t4")

    val sql4 =
      """
        |select
        |	area,product_name,clickcount,cityinfo
        |from t4
        |where rn <=3
        |""".stripMargin
    val df4: DataFrame = sparkSession.sql(sql4)
    df4.show(1000, false)

  }


}
