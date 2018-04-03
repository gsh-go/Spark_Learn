package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ScoketStraming {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args : Array[String]): Unit ={
    //创建一个本地的StreamingContext，含2个工作线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming")
    val sc = new StreamingContext(conf,Seconds(10))
    //创建珍一个DStream，连接master:9998
    val lines = sc.socketTextStream("localhost",9998)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x,1)).reduceByKey(_+_)
    wordCounts.print()
    sc.start()
    //通过手动终止计算，否则一直运行下去
    sc.awaitTermination()

  }

}
