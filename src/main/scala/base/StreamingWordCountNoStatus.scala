package base

import org.apache.spark._
import org.apache.spark.streaming._

/**
  * Created by kali on 7/3/16.
  */
object StreamingWordCountNoStatus {
  def main(args: Array[String]): Unit ={
    if (args.length < 2)
      System.err.println("Usage: WordCount <host> <port>")
      System.exit(1)

    val conf = new SparkConf().setAppName("StreamingWordCountNoStatus")
    val ssc = new StreamingContext(conf,Seconds(2))

    val lines = ssc.socketTextStream(args(0),args(1).toInt)

    val wordcount = lines.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)

    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
