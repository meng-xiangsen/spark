package day10

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.junit.Test

/*class DStreamOperationTest {
  /*
       每间隔3秒，计算过去1h的数据！
             相邻两个Job有大量重复的数据，可以基于有状态的计算，提高效率！

             有状态
  */





  /*三个和时间相关的概念
            以wordcount为例：
              采集周期：   在构建context时决定   3秒
              Job提交的间隔(滑动步长)：   默认和采集周期一致 3秒
              窗口(计算的数据的范围)：  默认和采集周期一致  3秒

              滑动步长和窗口必须是采集周期的整倍数

           需求：每间隔3秒，统计最新6秒的数据！
                    滑动步长： 3秒
                    窗口：  6秒
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,  : 聚合函数
      windowDuration: Duration   ： 窗口的周期大小
    ): DStream[(K, V)]

   */
  @Test

  def testReduceByKeyAndWindow(): Unit ={
    val ssc = new StreamingContext("local[*]", "wc", Seconds(3))
    val ds = ssc.socketTextStream("hadoop102", 9999)
    val ds2 = ds.map(line => (line, 1))
    //每间隔3秒，统计最新6秒的数据！
    val ds3 = ds2.reduceByKeyAndWindow(_ + _, Seconds(6))
    //每间隔6秒，统计最新6秒的数据！
    val ds4 = ds2.reduceByKeyAndWindow(_ + _, windowDuration = Seconds(6), slideDuration = Seconds(6))
    ssc.start()
    ssc.awaitTermination()
  }

  @Test
  def testStop(): Unit ={
    val ssc = new StreamingContext("local[*]", "wc", Seconds(3))
    ssc.start()
    //streamingContext1.stop()  not ok
    //启动一个分线程，在分线程中判断当应用需要关闭时，再执行stop
    new Thread(){
      override def run(): Unit = {
        def getFlag():Boolean={
          return true
        }
        while(getFlag){
          Thread.sleep(5000)
        }
        ssc.stop(true,true)

      }
    }.start()



    ssc.awaitTermination()
  }


  @Test
  def testUpdateStateByKey(): Unit ={
    val ssc1 = new StreamingContext("local[*]", "1111", Seconds(3))
    ssc1.checkpoint("ck")

    val ds1 = ssc1.socketTextStream("hadoop102", 9999)
    val ds3 = ds1.map(line => (line, 1))
    //updateFunc: (Seq[V], Option[S]) => Option[S]
    val ds4 = ds3.updateStateByKey((values: Seq[Int], state: Option[Int]) => Some(values.sum + state.getOrElse(0)))

    ds4.print(1000)


    ssc1.start()

    ssc1.awaitTermination()
  }




  @Test
  def testJoin(): Unit ={
    val ssc1 = new StreamingContext("local[*]", "1111", Seconds(3))

    val ds1 = ssc1.socketTextStream("hadoop102", 9999)
    val ds2 = ssc1.socketTextStream("hadoop102", 9998)
    val ds3 = ds1.map(line => (line, 1))
    val ds4 = ds2.map(line => (line, 2))
    val ds5 = ds3.join(ds4)
    ds5.print(1000)




    ssc1.start()

    ssc1.awaitTermination()

  }

  @Test
  def testTransform(): Unit = {
    val ssc = new StreamingContext("local[*]", "1111", Seconds(3))
    val ds = ssc.socketTextStream("hadoop102", 9999)
    val ds1 = ds.transform(rdd => {
      rdd.map(str => str.toInt)
    })
    ds1.print(100)
    ssc.start()
    ssc.awaitTermination()

  }

}*/
