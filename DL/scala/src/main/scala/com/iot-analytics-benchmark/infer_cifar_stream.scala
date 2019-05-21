/*
infer_cifar_stream.scala: read pre-labeled CIFAR10 images using Spark Streaming, infer object using saved pre-trained Resnet model, compare to label - scala version
                           with workarounds from Intel for model.predictClass issue with Spark Streaming

Usage:

In one window, run send_images_cifar_stream.scala (see that program for details)

In a second window:
  $ spark-submit <Spark config params> --class com.intel.analytics.bigdl.models.resnet.infer_cifar_stream <path>/iotstreamdl-assembly-0.0.1.jar <arguments>

  Arguments:
  -r, --reportingInterval <value> reporting interval (sec)   Default: 1
  -i, --sourceIPAddress <value>   source IP address          Default: 192.168.1.1 
  -p, --sourcePort <value>        source port                Default: 10000
  -m, --model <value>             model                      Required
  -b, --batchSize <value>         batch size                 Default: 2000

Uses Intel's BigDL library (https://github.com/intel-analytics/BigDL) and CIFAR10 dataset from https://www.cs.toronto.edu/~kriz/cifar.html
(Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009, https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf)

Copyright (c) 2019 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package com.intel.analytics.bigdl.models.resnet

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.bigdl.models.resnet.Utils._
import com.intel.analytics.bigdl.dataset.image.{BGRImgNormalizer, BGRImgToSample, BytesToBGRImg}
import com.intel.analytics.bigdl.dataset.{ByteRecord, Transformer, Sample}
import com.intel.analytics.bigdl.transform.vision.image.{ImageFeature,ImageFrame}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.optim.{Top1Accuracy}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import scopt.OptionParser
import java.time._

object SampleToImagefeature {
  def apply(): SampleToImagefeature = {
    new SampleToImagefeature()
  }
}

class SampleToImagefeature() extends Transformer[Sample[Float], ImageFeature] {

  override def apply(prev: Iterator[Sample[Float]]): Iterator[ImageFeature] = {
    prev.map(img => {
      val imageFeature = new ImageFeature()
      val featureBuffer = Tensor[Float]()
      val labelBuffer = Tensor[Float]()

      featureBuffer.resizeAs(img.feature()).copy(img.feature())
      labelBuffer.resizeAs(img.label()).copy(img.label())
      imageFeature.update("sample", Sample(featureBuffer, labelBuffer))
      imageFeature.update("label", labelBuffer)
      imageFeature
    })
  }
}

object infer_cifar_stream {

  def main(args: Array[String]) {

    case class Params(
      reportingInterval: Int  = 10,
      sourceIPAddress: String = "192.168.1.1",
      sourcePort: Int = 10000,
      model: String = "",
      batchSize: Int = 2000
    )

    val parser = new OptionParser[Params]("infer_cifar_stream") {
      opt[Int]('r', "reportingInterval")
        .text("reporting interval")
        .action((x, c) => c.copy(reportingInterval = x))
      opt[String]('i', "sourceIPAddress")
        .text("source IP address")
        .action((x, c) => c.copy(sourceIPAddress = x))
      opt[Int]('p', "sourcePort")
        .text("source port")
        .action((x, c) => c.copy(sourcePort = x))
      opt[String]('m', "model")
        .text("model")
        .action((x, c) => c.copy(model = x))
        .required
     opt[Int]('b', "batchSize")
        .text("batch size")
        .action((x, c) => c.copy(batchSize = x))
     }

    parser.parse(args, Params()).foreach { param =>

      println("%s: Classifying images from %s:%d with Resnet model %s, with %d second intervals"
        .format(Instant.now.toString, param.sourceIPAddress, param.sourcePort, param.model, param.reportingInterval))

      // Initialize BigDL engine, SparkContext, and various accumulators
      val conf = Engine.createSparkConf().setAppName("infer_cifar_stream")
      val sc = new SparkContext(conf)
      val interval = sc.accumulator(0)
      val empty_intervals = sc.accumulator(0)
      val images  = sc.accumulator(0)
      val tot_correct_preds  = sc.accumulator(0)
      val tot_correct_eval  = sc.accumulator(0)

      Engine.init
      val partitionNum = Engine.nodeNumber() * Engine.coreNumber()

      // Initialize StreamingContext, have it read TextStream through socket
      val ssc = new StreamingContext(sc, Seconds(param.reportingInterval))
      val image_stream = ssc.socketTextStream(param.sourceIPAddress, param.sourcePort)

      // Parse individual labeled image string into a ByteRecord consisting of the image data and label
      def parse_labeled_image_string(labeled_image_string: String): ByteRecord = {
        val idx = labeled_image_string.indexOf(",")
        val label_string = labeled_image_string.substring(0, idx)
        val data_string = labeled_image_string.substring(idx+1)
        val image_data_array = data_string.split(",").map(_.toByte)
        ByteRecord(image_data_array, label_string.toFloat)
      }
  
      def run_model(rdd: RDD[String]): Unit = {
        // Input rdds consist of batches of labeled images encoded as strings 
        // Skip empty intervals and stop on first empty interval after start
        if (rdd.count == 0) {
          empty_intervals.add(1)
          println("%s: No input".format(Instant.now.toString))
          if (interval.value > 0) {
            println("%s: Stopping stream".format(Instant.now.toString))
            ssc.stop()
          }
        }
        else {
          // Non-empty interval
          interval.add(1)
          val input_length = rdd.count.toInt
          images.add(input_length)
          // Parse each line of rdd (labeled images) and repartition result - BigDL requires total batch be divisible by partitionNum
          val rddData= rdd.map(parse_labeled_image_string).repartition(partitionNum)
          // Transform data into form required by Resnet model
//        val transformer = BytesToBGRImg() -> BGRImgNormalizer(Cifar10DataSet.trainMean, Cifar10DataSet.trainStd) -> BGRImgToSample()
          val transformer = BytesToBGRImg() -> BGRImgNormalizer(Cifar10DataSet.trainMean, Cifar10DataSet.trainStd) -> BGRImgToSample() -> SampleToImagefeature()
          val evaluationSet = transformer(rddData)
//        val labels = evaluationSet.map(_.label).map(l => l.value.toInt).collect
          val model = Module.load[Float](param.model)
//        val predictions = model.predictClass(evaluationSet, param.batchSize).collect
//        var correct_preds = 0
//        for (i <- 0 to input_length-1) { if (predictions(i) == labels(i)) correct_preds +=1 }
          val result = model.predictImage(ImageFrame.rdd(evaluationSet), batchPerPartition = param.batchSize / partitionNum)
          val correct_preds = result.toDistributed().rdd.mapPartitions(res => {
            res.map(feature => {
              val label = feature.apply[Tensor[Float]]("label")
              val predict = feature.apply[Tensor[Float]]("predict").max(1)._2
              if (label.valueAt(1) == predict.valueAt(1)) 1 else 0
            })
          }).reduce((left, right) => {
            left + right
          })
          tot_correct_preds.add(correct_preds)
          println("%s: %d images received in interval - %d or %.1f%% predicted correctly".format(Instant.now.toString, input_length, correct_preds, 100.0*correct_preds/input_length))
        }
      }

      // Run model on each batch
      image_stream.foreachRDD(run_model(_))

      // Start reading streaming data
      ssc.start()
      val start_time = System.nanoTime
      ssc.awaitTermination()
      val finish_time = System.nanoTime
      // Subtract off time waiting for images and 2 sec for termination
      val elapsed_time = (finish_time - start_time)/1000000000.0  - empty_intervals.value*param.reportingInterval - 2.0
      print("\n%s: %d images received in %.1f seconds (%d intervals), or %.0f images/second. "
        .format(Instant.now.toString, images.value, elapsed_time, interval.value, images.value.toFloat/elapsed_time))
      println("%d of %d or %.1f%% predicted correctly ".format(tot_correct_preds.value, images.value, 100.0*tot_correct_preds.value/images.value))
    }
  }
}
