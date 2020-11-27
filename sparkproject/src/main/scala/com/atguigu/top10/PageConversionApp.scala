package com.atguigu.top10

import java.text.DecimalFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageConversionApp {

  def PageConversionRate(
                          sc:SparkContext,
                          userVisitActionRDD: RDD[UserVisitAction],
                          pagesString:String
                        )={
    // 1. 先把需要计算跳转率的页面id切出来
    val pages = pagesString.split(",")
    val prePages = pages.init
    val postPages = pages.tail
    // 1.1 做出来目标跳转流
    val targetFlow = prePages.zip(postPages).map {
      case (pre, post) => s"$pre->$post"
    }
    // 2. 计算分母  : 1-6这个6个页面的访问量
    val pageAndCoung = userVisitActionRDD
      .filter(action => {
        prePages.contains(action.page_id.toString)
      })
      .map(action => (action.page_id, 1))
      .countByKey()

    // 3. 计算分子  "1->2"   "2->3"...
    val pageFlows = userVisitActionRDD
      .groupBy(_.session_id)
      .flatMap {
        case (sid, it) =>
          val actionSorted = it.toList.sortBy(_.action_time).map(_.page_id)
          val preActions = actionSorted.init
          val postActions = actionSorted.tail

          preActions
            .zip(postActions)
            .map {
              case (pre, post) => s"$pre->$post"
            }
            .filter(flow => targetFlow.contains(flow))
      }
      .map((_,1))
      .countByKey()
    // 4. 计算跳转流
    val f = new DecimalFormat(".00%")
    val res = pageFlows.map {
      case (flow, count) =>
        val fenzi = count
        val fenmu = pageAndCoung(flow.split("->")(0).toLong)

        (flow,f.format(fenzi.toDouble / fenmu ))
    }


  }
}
