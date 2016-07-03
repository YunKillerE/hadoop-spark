package base

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * Created by kali on 7/3/16.
  */
object StreamingWordCountWindow {
  def main(args: Array[String]): Unit ={
    if (args.length < 2)
      System.err.println("Usage: WordCount <file1>")
      System.exit(1)

    val conf = new SparkConf().setAppName("StreamingWordCountNoStatus")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("hdfs://user/spark/checkpint")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(args(0),args(1).toInt)

    val wordcount = lines.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKeyAndWindow((a,b)=> a + b,Seconds(10))

    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
