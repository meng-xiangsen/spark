package SparkStreaming.App

import SparkStreaming.Beans.AdsInfo
import SparkStreaming.Utils.JDBCUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object App2 extends BaseApp {


  //描述：实时统计每天各地区各城市各广告的点击总流量，并将其存入MySQL。
  // 输入 1604922609982,华东,上海,105,5
  def main(args: Array[String]): Unit = {


    runApp {
      ssc.checkpoint("app2ck")
      val ds: DStream[AdsInfo] = getDatas()
      val result = ds.map(bean => ((bean.dayString, bean.area, bean.city, bean.adsId), 1))
        .updateStateByKey((values: Seq[Int], state: Option[Int]) => Some(values.sum + state.getOrElse(0)))

      result.foreachRDD(rdd => rdd.foreachPartition(iter => {
        val connection = JDBCUtil.getConnection()
        val sql =
          """
            |
            |INSERT INTO area_city_ad_count VALUES(?,?,?,?,?)
            |ON DUPLICATE KEY UPDATE  COUNT=?
            |""".stripMargin
        val ps = connection.prepareStatement(sql)

        iter.foreach {
          case ((data, area, city, ads), count) => {
            ps.setString(1, data)
            ps.setString(2, area)
            ps.setString(3, city)
            ps.setString(4, ads)
            ps.setInt(5, count)
            ps.setInt(6, count)
          }
            ps.executeUpdate()
        }
        ps.close()
        connection.close()
      }))
    }
  }
}
