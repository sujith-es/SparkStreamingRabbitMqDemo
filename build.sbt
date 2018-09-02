name := "SparkStreamingRabbitMqDemo"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "com.rabbitmq" % "amqp-client" % "3.6.6",
  "com.typesafe.akka" %% "akka-actor" % "2.5.3",
  "junit" % "junit" % "4.8.1" ,
  "joda-time" % "joda-time" % "2.8.2"
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.stratio.receiver" % "spark-rabbitmq" % "0.5.1"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"