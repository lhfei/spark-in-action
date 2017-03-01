package cn.lhfei.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lihefei on 2/28/2017.
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      System.err.format("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split("_"))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_+_)

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
