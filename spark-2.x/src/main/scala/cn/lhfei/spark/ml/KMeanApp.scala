package cn.lhfei.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans

object KMeanApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("KMeans Sample")
      .master("local[1]")
      .getOrCreate();

    val dataset = spark.read.format("libsvm").load("/spark-data/data/mllib/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    spark.stop()
  }

}