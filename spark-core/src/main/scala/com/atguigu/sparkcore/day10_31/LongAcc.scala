package com.atguigu.sparkcore.day10_31

import org.apache.spark.util.AccumulatorV2

class LongAcc extends AccumulatorV2[Long,Long]{
  private var _sum = 0L
  private var _count = 0L
  override def isZero: Boolean = _sum == 0 && _count==0

  override def copy(): AccumulatorV2[Long, Long] = {
    val acc = new LongAcc
    acc._count = this._count
    acc._sum = this._sum
    acc
  }

  override def reset(): Unit = {
    _count== 0
    _sum == 0
  }

  override def add(v: Long): Unit = {
    _sum += v
    _count += 1
  }

  override def merge(other: AccumulatorV2[Long, Long]): Unit = {
        other match {

          case other:LongAcc =>{
            _sum = other._sum
          }
          case _ => throw new UnsupportedOperationException
        }
  }

  override def value: Long = _sum
}
