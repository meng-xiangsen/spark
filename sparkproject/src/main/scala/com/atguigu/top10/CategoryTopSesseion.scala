package com.atguigu.top10

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
/*
((cid, sid), 1)
=> reduceByKey ((cid, sid), count)
=> map (cid, (sid, count))
=> groupByKey (cid, List((sid, count), ..,))
=> map 对list做排序, 根据count取top10

----
我们要排序, 需要把iterable进行toList, 会有可能导致oom

第二种解法:
    使用spark的排序

    sortByKey
    sortBy
        特点: 对RDD中所有的数据整体排序

    我们的要求: 每个品类单独排序


    把RDD过滤到只有一个品类, 然后再排序, 计算结果
    好处: 数据量即使很大一定可以排序成功, 不会oom
    坏处: 需求遍历10次, 起10个job(???job其实一个job也可以,使用union完成, 取topN很难办)

    如果品类比较少, 可以使用这种方案


 第3和第4:
    使用treeset做排序


    解法3:
        多一个shuffle

    解法4:
        把groupByKey去掉

        分区器
            一个品类一个分区

         如果品类过多, 则分区数会过多



 */
object CategoryTopSesseion {
  def statTop10Session_1(
                        sc:SparkContext,
                        userVisitActionRDD:RDD [UserVisitAction],
                        top10: List[CategoryCountInfo])={
    val top10CategoryActionRdd = userVisitActionRDD.filter(action => {
      top10.map(_.categoryId.toLong).contains(action.click_category_id)
    })

    val top10CategoryActionAggendRdd = top10CategoryActionRdd.map(action => {
      (action.click_category_id, action.session_id) -> 1L
    })
      .reduceByKey(_ + _)
      .map {
        case ((cid, sid), count) => cid -> (sid, count)
      }
      .groupByKey()

    val result = top10CategoryActionAggendRdd.map({
      case (cid, it) => cid -> it.toList.sortBy(-_._2).take(3)
    })

    result.foreach(println)
  }

  def statTop10Session_2(
    sc:SparkContext,
    userVisitActionRDD:RDD [UserVisitAction],
    top10: List[CategoryCountInfo])={

    val cids = top10.map(_.categoryId.toLong)
    // 每个品类下, 每个session的点击量计算出来
    // 1. 过滤出来, 只包含top10品类的点击记录
    val top10CategoryActionRDD = userVisitActionRDD.filter(action => {
      cids.contains(action.click_category_id)
    })
    // 2. 聚合

    val top10CategoryActionAggedRDD = top10CategoryActionRDD.map(action => {
      (action.click_category_id, action.session_id) -> 1L
    })
      .reduceByKey(_ + _)
      .map({
        case ((cid, sid), count) => cid -> (sid, count)
      })

    top10CategoryActionAggedRDD.cache()
    var res = List[(Long, List[(String, Long)])]()

    // 遍历每个品类, 使用spark的排序, 计算top(N)
    cids.foreach(f = cid => {
      val array = top10CategoryActionAggedRDD
        .filter(_._1 == cid)
        .sortBy(-_._2._2)
        .map(_._2)
        .take(3)


      res = (cid, array.toList) :: res
    })






  }

  def statTop10Session_3(
                          sc:SparkContext,
                          userVisitActionRDD:RDD [UserVisitAction],
                          top10: List[CategoryCountInfo])={
    val cids: List[Long] = top10.map(_.categoryId.toLong)
    // 每个品类下, 每个session的点击量计算出来
    // 1. 过滤出来, 只包含top10品类的点击记录
    val top10CategoryActionRDD = userVisitActionRDD.filter(action => {
      cids.contains(action.click_category_id)
    })
    // 2. 聚合

    val top10CategoryActionAggedRDD = top10CategoryActionRDD.map(x => {
      (x.click_category_id, x.session_id) -> 1L
    }).reduceByKey(new CategoryPartitioner(cids),_ + _).map {
      case ((cid, sid), count) => cid -> (sid, count)
    }.groupByKey()
    // 3. 排序, 取top10
    val result = top10CategoryActionAggedRDD.map {
      case (cid, it: Iterable[(String, Long)]) => {
        var infoSet = mutable.TreeSet[SessionInfo]()
        it.foreach {
          case (sid, count) =>
            infoSet += SessionInfo(cid, sid, count)
            if (infoSet.size > 3)
              infoSet = infoSet.take(3)

        }
        infoSet
      }
    }
    result.collect.foreach(println)




  }


  def statTop10Session_4(
                          sc:SparkContext,
                          userVisitActionRDD:RDD [UserVisitAction],
                          top10: List[CategoryCountInfo])={
    val cids: List[Long] = top10.map(_.categoryId.toLong)
    // 每个品类下, 每个session的点击量计算出来
    // 1. 过滤出来, 只包含top10品类的点击记录
    val top10CategoryActionRDD = userVisitActionRDD.filter(action => {
      cids.contains(action.click_category_id)
    })
    // 2. 聚合

    val top10CategoryActionAggedRDD = top10CategoryActionRDD.map(x => {
      (x.click_category_id, x.session_id) -> 1L
    }).reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }


    val res = top10CategoryActionAggedRDD.mapPartitions((it: Iterator[(Long, (String, Long))]) => {
      var infoSet = mutable.TreeSet[SessionInfo]()

      it.foreach({
        case (cid, (sid, count)) =>
          infoSet += SessionInfo(cid, sid, count)
          if (infoSet.size > 3)
            infoSet = infoSet.take(3)
      })
      infoSet.iterator
    })

    res.collect.foreach(println)
  }
}
class CategoryPartitioner(cids: List[Long]) extends Partitioner{
  private val cidIndex= cids.zipWithIndex.toMap

  override def numPartitions: Int = cids.size

  override def getPartition(key: Any): Int = {
    // 根据key获取索引
    //cidIndex(key.asInstanceOf[Long])
    key match {
      case (cid:Long,sid) => cidIndex(cid)
      case _ =>throw new UnsupportedOperationException
    }


  }
}