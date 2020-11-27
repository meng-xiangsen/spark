package com.atguigu.top10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Top10 {


   def getTop10_1(sc:SparkContext,actionRdd:RDD[UserVisitAction]) = {
    //CategoryCountInfo(鞋,1,0,0)
    //CategoryCountInfo(鞋,0,1,0)
    //CategoryCountInfo(鞋,0,0,1)
    //=>希望变成：CategoryCountInfo(鞋,1,1,1)
    //3.3 将转换结构后的数据进行分解成CategoryCountInfo
    val infoRdd: RDD[CategoryCountInfo] = actionRdd.flatMap {

      case act: UserVisitAction => {

        if (act.click_category_id != -1) { // 点击信息处理
          List(CategoryCountInfo(act.click_category_id.toString, 1, 0, 0))
        } else if (act.order_category_ids != "null") { // 订单信息处理

          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]

          val ids: Array[String] = act.order_category_ids.split(",")

          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 1, 0))
          }
          list
        } else if (act.pay_category_ids != "null") { // 支付信息处理

          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]

          val ids: Array[String] = act.pay_category_ids.split(",")

          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 0, 1))
          }
          list
        } else {
          Nil
        }
      }
    }
    /*val infoRdd:RDD[CategoryCountInfo]={
      case act:UserVisitAction=> { // 点击信息处理
        if (act.click_category_id != -1) {
          List(CategoryCountInfo(act.click_category_id.toString, 1, 0, 0))
        } else if (act.order_category_ids != "null") { // 订单信息处理
          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          val ids: Array[String] = act.order_category_ids.split(",")
          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 1, 0))
          }
          list
        } else if (act.pay_category_ids != "null") { // 支付信息处理

          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]

          val ids: Array[String] = act.pay_category_ids.split(",")

          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 0, 1))
          }
          list
        } else {
          Nil
        }
      }
      case _ =>throw new UnsupportedOperationException
    }*/
    //3.4 将相同的品类分成一组
    val gorupRdd = infoRdd.groupBy(info => info.categoryId)

    //3.5 将分组后的数据进行聚合处理: (品类id, (品类id, clickCount, OrderCount, PayCount))
    val mapRdd = gorupRdd.mapValues(
      datas => {
        datas.reduce(
          (info1, info2) => {
            info1.orderCount = info1.orderCount + info2.orderCount
            info1.clickCount = info1.clickCount + info2.clickCount
            info1.payCount = info1.payCount + info2.payCount
            info1
          }
        )
      }
    ).map(_._2)

    val res = mapRdd.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)

    res.foreach(println)

    res.toList
  }
  def getTop10_2(sc:SparkContext,actionRdd:RDD[UserVisitAction])={

    val acc = new CategoryAcc
    //3.6 注册累加器
    sc.register(acc,"getTop10Acc")

    //actionRdd.foreach(action => acc.add(action))
    //3.7 累加器添加数据
    actionRdd.foreach(acc.add)
    //3.8 获取累加器的值
    val accMap = acc.value
    val res = accMap.
      sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount))
      .take(10)
res
  }

}


