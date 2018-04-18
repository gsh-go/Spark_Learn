package streaming


import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */

object SimpleStreaming {

  //Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()

    //val Array(brokers, topics) = args
    val brokers = "192.168.123.128:9092"
    val topics = "stream-test"
    val group="con-consumer-group"

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
      //创建一个本地 含2个工作线程
      .setMaster("local[2]")
      //.setMaster("spark://localhost:7077")
      .setJars(List("/home/gsh/WorkSpaces/Spark_Learn/out/artifacts/Spark_Learn_jar/Spark_Learn.jar"))

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //val kafkaParams = Map[String, String]("bootstrap.servers" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    //stream.map(s =>(s.key(),s.value())).print();

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }



}
