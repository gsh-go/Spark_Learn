package ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

object KMeansExample {


  def main(args :Array[String ]): Unit ={

/*     val conf = new SparkConf().setAppName("Spark Pi")
    .setMaster("spark://localhost:7077")
    .setJars(List("/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar"))

  val spark = new SparkContext(conf)*/


    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      //.master("spark://localhost:7077")
      .config("spark.jars", "/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar")
      .getOrCreate();

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm").load("/opt/spark-2.3.0-bin-hadoop2.7/data/mllib/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Make predictions
    val predictions = model.transform(dataset)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    spark.stop()





  }

}
