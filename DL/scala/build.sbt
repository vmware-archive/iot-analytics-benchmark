name := "iotstreamdl"

version := "0.0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"
val BigDLVersion = "0.7.0"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided", 
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.intel.analytics.bigdl" % "bigdl-SPARK_2.3" % BigDLVersion,
  "org.apache.hadoop" % "hadoop-common" % "3.0.0",
)
