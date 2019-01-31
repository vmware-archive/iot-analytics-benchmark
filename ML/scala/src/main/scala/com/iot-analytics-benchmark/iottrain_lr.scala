/*
iottrain_lr.scala: Spark program to train and generate logistic regression model from pre-collected sensor data - scala version
Reads training data and stores model on HDFS or S3
Usage: spark-submit --name iottrain_lr --class com.iotstream.iottrain_lr <path>iotstream_<scala version>-<code version>.jar HDFS_or_S3 HDFS_path_or_S3_bucket filename modelname

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package com.iotstream

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import java.time.Instant

object iottrain_lr {
  def main(args: Array[String]) {
  
    if (args.length != 4) {
      System.err.println("Usage: spark-submit --name iottrain_lr --class com.iotstream.iottrain_lr <path>iotstream_<scala version>-<code version>.jar HDFS_or_S3 HDFS_path_or_S3_bucket filename modelname")
      System.exit(-1)
    }
  
    val ifilename= {
      if (args(0).capitalize == "S3") "s3a://%s/%s".format(args(1), args(2))
      else "%s/%s".format(args(1), args(2))
    }
  
    val ofilename= {
      if (args(0).capitalize == "S3") "s3a://%s/%s".format(args(1), args(3))
      else "%s/%s".format(args(1), args(3))
    }
  
    println("%s: Training logistic regression model and storing as %s using data from %s".format(Instant.now.toString, ofilename, ifilename))

    val start_time = System.nanoTime
  
    def parsePoint(line: String): LabeledPoint = {
      val values = for (a <- line.split(",")) yield a.toDouble
      LabeledPoint(values(0), Vectors.dense(values.drop(1)))
    }

    val conf = new SparkConf().setAppName("iottrain_lr")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile(ifilename)
    val parsedData = data.map(parsePoint)

    // Build the model
    val model = new LogisticRegressionWithLBFGS().run(parsedData)

    // Save model
    model.save(sc, ofilename)

    val elapsed_time = (System.nanoTime - start_time)/1000000000.0
    println("%s: Trained logistic regression model and stored as %s in %.1f seconds".format(Instant.now.toString, ofilename, elapsed_time))
  
    sc.stop()
  }
}
