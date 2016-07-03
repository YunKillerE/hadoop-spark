package base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kali on 7/2/16.
  */
object WordCount {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: WordCount <file1>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    //word count
    val rdd = sc.textFile(args(0)).flatMap(_.split("\t")).map(x=>(x,1)).reduceByKey(_+_)
    val rdd1: RDD[(String, Int)] = sc.textFile(args(0)).flatMap(_.split("\t")).map(x=>(x,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(ascending = false).map(x=>(x._2,x._1))
    rdd.collect()
    //print "========================================================="
    rdd1.collect()

  }

}
