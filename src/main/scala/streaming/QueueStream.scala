package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

/**
  * Spark Streaming提供了表示连续数据流的、高度抽象的被称为离散流的DStream。
  * DStream本质上表示RDD的序列。
  * 任何对DStream的操作都会转变为对底层RDD的操作。
  */
object QueueStream {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("QueueStream")
      .setMaster("local[2]")
      //.setMaster("spark://localhost:7077")
      .setJars(List("/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar"))

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)//DStream不支持sortByKey()
    reducedStream.print()
    ssc.start()

    // Create and push some RDDs into rddQueue
    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(10000)
    }
    ssc.start()
    ssc.awaitTermination()
    //ssc.stop()
  }

}
