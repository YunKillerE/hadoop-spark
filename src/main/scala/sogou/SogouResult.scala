package sogou

import org.apache.spark.{SparkConf, SparkContext}

object SogouResult{
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SogouResult <file1> <file2>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SogouResult").setMaster("local")
    val sc = new SparkContext(conf)

    //session查询次数排行榜
    val rdd1 = sc.textFile(args(0)).map(_.split("\t")).filter(_.length==6)
    val rdd2=rdd1.map(x=>(x(1),1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    rdd2.saveAsTextFile(args(1))
    sc.stop()
  }
}