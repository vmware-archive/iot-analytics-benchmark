/*
iotgen_lr.scala: Spark program to generate CSV sensor data for training logistic regression model
  Breaks computation into specified number of partitions, each partition's data is converted to CSV and then written out
  Stores output file on HDFS or S3
Usage: spark-submit --name iotgen_lr --class com.iotstream.iotgen_lr <path>iotstream_<scala version>-<code version>.jar n_rows n_sensors n_partitions HDFS_or_S3 HDFS_path_or_S3_bucket filename <cutoff>
For exact number of rows make n_rows integer multiple of n_partitions
If cutoff not specified will generate 50% 1 labels - use calc_cutoffs.py to calculate other percentages

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package com.iotstream

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.math._
import Array._
import java.util.Random
import java.time.Instant

object iotgen_lr {
  def main(args: Array[String]) {
  
    if (!(args.length == 6 || args.length == 7)) {
      System.err.println("Usage: spark-submit --name iotgen_lr --class com.iotstream.iotgen_lr <path>iotstream_<scala version>-<code version>.jar n_rows n_sensors n_partitions HDFS_or_S3 HDFS_path_or_S3_bucket filename <cutoff>")
      System.exit(-1)
    }
  
    var n_rows    = args(0).toInt
    val n_sensors = args(1).toInt
    val n_partitions  = args(2).toInt
  
    val ofilename= {
      if (args(3).capitalize == "S3") "s3a://%s/%s".format(args(4), args(5))
      else "%s/%s".format(args(4), args(5))
    }
  
    val cutoff = {
    if (args.length == 7) args(6).toDouble
    else .25 * n_sensors * (n_sensors+1)
    }
  
    val partition_size = ceil(n_rows.toFloat/n_partitions.toFloat).toInt  // In case n_rows not integer multiple of n_partitions
    n_rows = partition_size * n_partitions

    println("%s: Creating file %s with %d rows of %d sensors, each row preceded by score using cutoff %.1f, in %d partitions".format(Instant.now.toString, ofilename, n_rows, n_sensors, cutoff, n_partitions))
  
    val conf = new SparkConf().setAppName("iotgen_lr")
    val sc = new SparkContext(conf)
    val ones = sc.accumulator(0)

    def create_sensor_data_partition(i_partition: Int): Array[Array[Float]] = {
      var sensor_array = ofDim[Float](partition_size, n_sensors+1)
      val rand = new Random
      for (i <- 0 to partition_size-1) {
        var sensors = new Array[Float](n_sensors+1)
        // Assign a random number between 0 and 1 to each sensor value
        for (s <- 1 to n_sensors) {
          sensors(s) = rand.nextFloat
        }
        // Weight score by sensor number
        var score = 0.0
        for (s <- 1 to n_sensors) {
          score += sensors(s)*(s+1)
        }
        // Assign a label
        if (score > cutoff) {
          ones.add(1)
          sensors(0) = 1
        }
        else {
          sensors(0) = 0
        }
        sensor_array(i) = sensors
      }
      sensor_array
    }
  
    def toCSVLine(float_array: Array[Float]): String = {
      val s = for (f <- float_array) yield "%.5f".format(f)
      s.mkString(",")
    }
  
    val start_time = System.nanoTime
    // Create an RDD with n_partitions elements, send each to create_sensor_data_partition, combine results, convert to CSV output and save to ofilename
    val rdd = sc.parallelize(range(0, n_partitions), n_partitions)
    val lines = rdd.map(create_sensor_data_partition).flatMap(_.toList).map(toCSVLine)
    lines.saveAsTextFile(ofilename)
    val elapsed_time = (System.nanoTime - start_time)/1000000000.0

    val size = (n_sensors+1)*8*n_rows.toFloat
    var size_str = ""
    val TiB = pow(2,40); val GiB = pow(2,30); val MiB = pow(2,20); val KiB = pow(2,10)
    if (size >= TiB)       {size_str = "%.1fTB".format(size/TiB)}
    else if (size >= GiB)  {size_str = "%.1fGB".format(size/GiB)}
    else if (size >= MiB)  {size_str = "%.1fMB".format(size/MiB)}
    else if (size >= KiB)  {size_str = "%.1fKB".format(size/KiB)}
    println("%s: Created file %s with size %s in %.1f seconds with %d ones (%.1f%%)".format(Instant.now.toString, ofilename, size_str, elapsed_time, ones.value, (100.0*ones.value)/n_rows.toFloat))
  
    sc.stop()
  }
}
