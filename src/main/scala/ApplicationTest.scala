import org.apache.spark.SparkConf
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ApplicationTest {
  def main(args: Array[String]) {
    //RabbitMQ things
    val host = "localhost"
    val port = "15672"
    val queueName = "rabbitmq-queue"
    val vHost = "/"
    val userName = "clientsa"
    val password = "J1Z43fYGUmq26DpwN"
    val exchangeName = "test-exchangeName"
    val exchangeType = "direct"

    // Set hadoop home directory for windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    //Configure Spark properties
    val sparkConfig = new SparkConf()
      .setAppName("test")
      .setIfMissing("spark.master", "local[*]")

    sparkConfig.set("spark.driver.allowMultipleContexts", "true")
    sparkConfig.set("es.index.auto.create", "true")

    // Create Spark Streaming context
    val ssc = new StreamingContext(sparkConfig, Seconds(5))

    // Open connection to RabbitMq and read stream
    val receiverStream = RabbitMQUtils.createStream(ssc, Map(
      "host" -> host,
      "port" -> port,
      "queueName" -> queueName,
      "exchangeName" -> exchangeName,
      "exchangeType" -> exchangeType,
      "vHost" -> vHost,
      "userName" -> userName,
      "password" -> password
    ))

    val totalEvents = ssc.sparkContext.accumulator(0L, "Number of events received")

    receiverStream.start()
    println("started receiverStream")

    receiverStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val rdd1 = ssc.sparkContext.makeRDD(Seq(rdd.first()))

        // For debug can write to local file
         rdd1.saveAsTextFile("C:\\sparkscala\\straming")
        val count = rdd.count()
        // Do something with this message
        println(s"EVENTS COUNT : \t $count")
        totalEvents.add(count)
        //rdd.collect().sortBy(event => event.toInt).foreach(event => print(s"$event, "))
      } else println("RDD is empty")
      println(s"TOTAL EVENTS : \t $totalEvents")
    })

    // start and wait for messages to arrive at queue.
    ssc.start()
    ssc.awaitTermination()
  }
}