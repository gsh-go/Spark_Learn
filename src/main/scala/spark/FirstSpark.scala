package spark

import scala.math.random
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object FirstSpark {
  Logger.getLogger("org").setLevel(Level.ERROR)
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Spark Pi")
        .setMaster("spark://localhost:7077")
        .setJars(List("/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar"))

      val spark = new SparkContext(conf)
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = 100000 * slices
      val count = spark.parallelize(1 to n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
      println ("Pi is roughly " + 4.0 * count / n)
      spark.stop()

  }

}
