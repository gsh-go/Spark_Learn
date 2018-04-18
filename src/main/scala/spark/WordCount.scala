package spark

//scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
  * Spark版本的WordCount
  */
object WordCount {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main (arg:Array[String ]): Unit ={
    val spark = SparkSession.builder()
      .master("local[2]")
      //.master("spark://localhost:7077")
      .appName("Spark_WordCount")
      .config("spark.jars", "/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar")
      .getOrCreate();

    val sc = spark.sparkContext;
    val lines = sc.textFile("hdfs://localhost:9000/input/02b71fdb-f678-4351-bfec-dcba31f81b23.txt")
    lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).collect().foreach(x => println(x))


    spark.stop()

  }

}
