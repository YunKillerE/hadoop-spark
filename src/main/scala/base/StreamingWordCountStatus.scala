package base

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * Created by kali on 7/3/16.
  */
object StreamingWordCountStatus {
  def main(args: Array[String]): Unit ={
    if (args.length < 2)
      System.err.println("Usage: WordCount <file1>")
      System.exit(1)

    val updateCount = (values: Seq[Int], state: Option[Int]) => {
      // /StateFul需要定义的处理函数，第一个参数是本次进来的值，第二个是过去处理后保存的值
      val currentCount = values.sum //求和
      val previousCount = state.getOrElse(0)    // 如果过去没有 即取0
      Some(currentCount + previousCount)// 求和
    }

    val conf = new SparkConf().setAppName("StreamingWordCountNoStatus")
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.checkpoint("hdfs://user/spark/checkpint")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(args(0),args(1).toInt)

    val wordcount = lines.flatMap(_.split(" ")).map(x=>(x,1)).updateStateByKey[Int](updateCount)

    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
