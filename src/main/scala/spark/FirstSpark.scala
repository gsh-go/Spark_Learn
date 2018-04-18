package spark

import scala.math.random
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object FirstSpark {
  Logger.getLogger("org").setLevel(Level.ERROR)
    def main(args: Array[String]) {

      /**
        * Spark 2.0之前, SparkContext是Spark的主要切入点
        */
     /* val conf = new SparkConf().setAppName("Spark Pi")
        .setMaster("spark://localhost:7077")
        .setJars(List("/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar"))

      val spark = new SparkContext(conf)*/

      /**
        * Spark 2.0之后,SparkSession封装了SparkConf、SparkContext和SQLContext
        */

      val spark = SparkSession.builder()
        .master("local[2]")
        //.master("spark://localhost:7077")
        .appName("Spark Pi")
        .config("spark.jars", "/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar")
        .getOrCreate();
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = 100000 * slices

      val count = spark.sparkContext.parallelize(1 to n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
      println ("Pi is roughly " + 4.0 * count / n)
      spark.stop()

  }

}
