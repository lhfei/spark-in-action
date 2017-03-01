package cn.lhfei.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Hefei Li on 2/28/2017.
  */
object HdfsWordcount {

  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      System.err.println("Usage: HdfsWordCount <input file directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
