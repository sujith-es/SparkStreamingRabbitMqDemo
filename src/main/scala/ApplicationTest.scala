import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession._
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ApplicationTest {
  def main(args: Array[String]) {


    // Set hadoop home directory for windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val value = ConfigFactory.load()

    //RabbitMQ Configuration
    val host = value.getString("appConf.rabbitMqConfig.host")
    val port = value.getString("appConf.rabbitMqConfig.port")
    val queueName = value.getString("appConf.rabbitMqConfig.queueName")
    val vHost =value.getString("appConf.rabbitMqConfig.vHost")
    val userName = value.getString("appConf.rabbitMqConfig.userName")
    val password = value.getString("appConf.rabbitMqConfig.password")
    val exchangeName =value.getString("appConf.rabbitMqConfig.exchangeName")
    val exchangeType = value.getString("appConf.rabbitMqConfig.exchangeType")

    //Configure Spark properties
    val sparkConfig = new SparkConf()
      .setAppName("test")
      .setIfMissing("spark.master",value.getString("appConf.sparkConf.masterMode"))
      .set("es.index.auto.create", "true")
      .set("es.nodes", value.getString("appConf.elasticConfig.nodes"))
      .set("es.port", value.getString("appConf.elasticConfig.port"))
      .set("es.http.timeout", value.getString("appConf.elasticConfig.httpTimeOut"))
      .set("es.scroll.size", value.getString("appConf.elasticConfig.scrollSize"))


    sparkConfig.set("spark.driver.allowMultipleContexts", "true")
    sparkConfig.set("es.index.auto.create", "true")

    // Create Spark Streaming context
    val ssc = new StreamingContext(sparkConfig, Seconds(5))

    // Open connection to RabbitMq and read stream
    val receiverRabbitMqStream = RabbitMQUtils.createStream(ssc, Map(
      "host" -> host,
      "port" -> port,
      "queueName" -> queueName,
      "exchangeName" -> exchangeName,
      "exchangeType" -> exchangeType,
      "vHost" -> vHost,
      "userName" -> userName,
      "password" -> password
    ))

    //val sc = new SparkContext(sparkConfig)
    val sqlContext = builder().config(sparkConfig).getOrCreate();

    val totalEvents = ssc.sparkContext.accumulator(0L, "Number of events received")

    receiverRabbitMqStream.start()
    println("started receiverStream")

    receiverRabbitMqStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val rdd1 = ssc.sparkContext.makeRDD(Seq(rdd.first()))

        import sqlContext.implicits._
        val validLine = rdd1.toDF()
        import org.elasticsearch.spark.sql._

        // **** Write to ElasticSearch ******
        val indexName = value.getString("appConf.elasticConfig.indexName")
        val indexType = value.getString("appConf.elasticConfig.indexType")
        validLine.saveToEs(indexName + "/" + indexType)

        // For debug can write to local file
        //validLine.rdd.saveAsTextFile("C:\\sparkscala\\straming")

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