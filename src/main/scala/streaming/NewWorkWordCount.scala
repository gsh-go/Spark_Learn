package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *   Usage: NetworkWordCount <hostname> <port>
  * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * and then run the example
  *    `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
  *
  */
object NewWorkWordCount {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main (arg:Array[String ]): Unit ={
    val sparkConf = new SparkConf().setAppName("NetWorkWordCount")
      .setMaster("local[2]")
      //.setMaster("spark://localhost:7077")
      .setJars(List("/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar"))

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.socketTextStream("localhost","9999".toInt,StorageLevel.MEMORY_AND_DISK_SER)
    lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
