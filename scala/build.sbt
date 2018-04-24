name := "iotstream"

version := "0.0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"
val kafkaVersion = "1.1.0"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion,
  "org.apache.kafka" %  "kafka-clients" % kafkaVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
)
