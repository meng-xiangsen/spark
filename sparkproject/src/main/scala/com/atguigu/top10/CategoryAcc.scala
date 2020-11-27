package com.atguigu.top10

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAcc extends AccumulatorV2[UserVisitAction,List[CategoryCountInfo]]{
  // map作为累加的临时变量          (cid, "click")-> 1000  (cid, "order")-> 1000 , ....
  private val map= mutable.Map[(String, String), Long]()
  override def isZero: Boolean =map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, List[CategoryCountInfo]] = {
    val acc = new CategoryAcc
    acc.map ++= this.map
    acc
  }

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){
      val k=v.click_category_id.toString -> "click"

      //map.put((v.click_category_id.toString,"click"),1L)
      map +=  k ->(map.getOrElse(k,0L)+1L)
    }else if(v.order_category_ids !="null"){
      val strings = v.order_category_ids.split(",")

      strings.foreach(cid=>{
        val k = cid -> "order"
        map += k -> (map.getOrElse(k,0L) + 1L)
      })

    }else if(v.pay_category_ids !="null"){
      val strings = v.pay_category_ids.split(",")
      strings.foreach(cid =>{
        val k = cid -> "pay"
        map += k -> (map.getOrElse(k, 0L) + 1L)
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, List[CategoryCountInfo]]): Unit = {
    other match {
      case other:CategoryAcc =>{
        val tm = this.map
        val om = other.map
        om.foreach{
          case (cidAction,count)=>
            //tm(cidAction) = tm.getOrElse(cidAction,0L) + count
            tm += cidAction ->(tm.getOrElse(cidAction,0L) + count)
        }
      }
      case _ => throw new UnsupportedOperationException
    }
  }

  override def value: List[CategoryCountInfo] = {
    this.map
      .groupBy(_._1._1)
      .map{
        case (cid,map)=>CategoryCountInfo(
          cid,
          //map.getOrElse((cid,"click"),0L)
          map.getOrElse(cid->"click",0L),
          map.getOrElse(cid->"order",0L),
          map.getOrElse(cid->"pay",0L)

        )
      }
      .toList
  }
}
