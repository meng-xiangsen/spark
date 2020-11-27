package SparkStreaming.App

import SparkStreaming.Beans.AdsInfo
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

//需求三：最近一小时广告点击量
//输入： 1604903340082,华中,杭州,100,2
//    输出：
// * 1：List [15:50->10,15:51->25,15:52->30]
// * 2：List [15:50->10,15:51->25,15:52->30]
// * 3：List [15:50->10,15:51->25,15:52->30]
object App3 extends BaseApp {
  def main(args: Array[String]): Unit = {
    runApp{

      val ds1 = getDatas()
      /* val rs2 = ds1.map(bean => (bean.hmString, bean.adsId) -> 1)
         .reduceByKeyAndWindow(_ + _, Minutes(60))*/
      val res = ds1.window(Minutes(60)).
        map(bean => (bean.hmString, bean.adsId) -> 1).
        reduceByKey(_ + _).map {
        case ((hmString, ads), count) => (ads, (hmString, count))
      }.groupByKey()
      res.print(1000)
    }
  }
}
