package SparkStreaming.App

import java.sql.ResultSet

import SparkStreaming.Utils.JDBCUtil

import scala.collection.mutable.ListBuffer

object App1 extends BaseApp {
  def main(args: Array[String]): Unit = {
    runApp{
      val ds = getDatas()
      val ds1 = ds.
        map(bean => (bean.userId, bean.adsId, bean.dayString) -> 1).
        reduceByKey(_ + _)

      ds1.foreachRDD(rdd => rdd.foreachPartition({
        iter => {
          val connection = JDBCUtil.getConnection()
          val sql =
            """
              | insert into user_ad_count values(?,?,?,?)
              |on duplicate key update count=?+count
              |""".stripMargin
          val ps1 = connection.prepareStatement(sql)
          iter.foreach {
            case ((userid,ads,ds),count) =>{
              ps1.setString(1,ds)
              ps1.setString(2,userid)
              ps1.setString(3,ads)
              ps1.setInt(4,count)
              ps1.setInt(5,count)

              ps1.executeUpdate()
            }
          }

          ps1.close()
          val sql2 =
            """
              |select userid from user_ad_count where count >20
              |""".stripMargin

          val ps2 = connection.prepareStatement(sql2)
          val resultSet: ResultSet = ps2.executeQuery()
          val BlackList = ListBuffer[String]()

          while (resultSet.next()){
            BlackList.append(resultSet.getString("userid"))
          }
          ps2.close()

          val sql3 =
            """
              |insert into black_list value(?)
              |on DUPLICATE key update userid=?
              |""".stripMargin
          val ps3 = connection.prepareStatement(sql3)
          BlackList.foreach(name =>{
            ps3.setString(1,name)
            ps3.setString(2,name)
            ps3.executeUpdate()
          })
          ps3.close()
          connection.close()
        }
      }))

    }
  }
}
