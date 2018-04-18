package sparkSQL

// This import is needed to use the $-notation

import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkSqlDemo {

  def main(args :Array[String ]): Unit ={

    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      //.master("spark://localhost:7077")
      .config("spark.jars", "/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar")
      .getOrCreate();

    val df = spark.read.json("hdfs://localhost:9000/input/employee.json")



    df.printSchema()

    df.select("name").show






  }


}
