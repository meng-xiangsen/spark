package SparkSqlExer

import java.text.DecimalFormat

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

/**
 * *    自定义函数：
 * *        2.0  :  UserDefinedAggregateFunction 和 Aggregator
 * *        3.0  :  Aggregator
 * *
 * *        Aggregator[IN,BUF,OUT]
 * *        IN: 输入的数据
 * *                cityName  String
 * *        BUF：  聚合的缓存区
 * *                  每个城市的点击量: Map[String,Int]
 * *                  当前区域总点击量：  Int
 * *        OUT：输出   北京21.2%，天津13.2%，其他65.6%  String
 * *
 * *    函数定义完成后名称为 myudaf
 * *
 * *    select myudaf(cityName )  在分组后执行！ 先将数据按照 地区和商品分组
 * *
 * *    地区    商品    cityName
 * *    华北    A      北京
 * *    华北    A      北京
 * *    华北    A      北京
 * *    华北    A      天津
 * *    华北    A      北京
 * *    ------------------------------------
 * *    华南    A      广东
 * *    ....
 * *
 */
class MyUdaf extends Aggregator[String,Buf,String]{
  override def zero: Buf = Buf(0,mutable.Map[String,Int]())

  override def reduce(b: Buf, a: String): Buf = {
    b.sumCount += 1

    val precount = b.cityCount.getOrElse(a, 0)
    b.cityCount.put(a,precount + 1)
    b
  }

  override def merge(b1: Buf, b2: Buf): Buf = {
    b1.sumCount += b2.sumCount
    for ((cityName,b2Count) <- b2.cityCount) {
      val b1pre = b1.cityCount.getOrElse(cityName,0)
      b1.cityCount.put(cityName,b1pre + b2Count)

    }
    b1
  }

  override def finish(reduction: Buf): String = {
    //获取每个城市的点击的信息
    val sortedList = reduction.cityCount.toList.sortBy(-_._2)
    var result = ""
    val decimalFormat = new DecimalFormat(".0%")
    if(sortedList.size >=2){
      val otherCount = reduction.sumCount - sortedList(0)._2 - sortedList(1)._2
      result  += sortedList(0)._1 + decimalFormat.format(sortedList(0)._2.toDouble/reduction.sumCount)+","+
        sortedList(1)._1 + decimalFormat.format(sortedList(1)._2.toDouble/reduction.sumCount) + "," +
        "其他"+ decimalFormat.format(otherCount.toDouble/reduction.sumCount)
      result
    }else{
      result += sortedList(0)._1 + decimalFormat.format(sortedList(0)._2.toDouble/reduction.sumCount)
      result
    }


  }

  override def bufferEncoder: Encoder[Buf] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}

case class Buf(var sumCount:Int,var cityCount:mutable.Map[String,Int])
