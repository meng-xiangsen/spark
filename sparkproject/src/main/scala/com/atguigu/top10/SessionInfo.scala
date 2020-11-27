package com.atguigu.top10

case class SessionInfo(cid: Long, sid: String, count: Long) extends Ordered[SessionInfo]{
  override def compare(that: SessionInfo): Int = {
    if(this.count < that.count) 1 else -1
  }

}
