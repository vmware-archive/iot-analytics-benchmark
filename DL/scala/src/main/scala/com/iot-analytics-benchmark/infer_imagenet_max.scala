/*
infer_imagenet_max.scala: read pre-labeled ImageNet images from memory, infer image classification using saved pre-trained Resnet model, compare to label

Uses ResNet50 trained model from Facebook: https://github.com/facebook/fb.resnet.torch/tree/master/pretrained#trained-resnet-torch-models

Based on https://github.com/intel-analytics/BigDL/blob/master/spark/dl/src/main/scala/com/intel/analytics/bigdl/example/imageclassification/ImagePredictor.scala

Usage:
  $ spark-submit <Spark config params> --class com.intel.analytics.bigdl.examples.imageclassification.infer_imagenet_max <path>/iotstreamdl-assembly-0.0.1.jar <arguments>
  Arguments:
  -f, --folder    <value>  location of test image data  Required
  -m, --modelPath <value>  location of model            Required
  -d, --duration  <value>  duration (sec)               Default: 60
  -b, --batchSize <value>  batch size                   Default: number of executors

Uses Intel's BigDL library (https://github.com/intel-analytics/BigDL) and ImageNet dataset (http://image-net.org)

Copyright (c) 2019 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package com.intel.analytics.bigdl.example.imageclassification

import com.intel.analytics.bigdl.dataset.image._
import com.intel.analytics.bigdl.dataset.Transformer
import com.intel.analytics.bigdl.dlframes.DLClassifierModel
import com.intel.analytics.bigdl.example.imageclassification.MlUtils._
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.utils.{Engine, LoggerFilter}
import com.intel.analytics.bigdl.Module
import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.DenseVector
import java.nio.file.{Files, Path, Paths}
import java.time._
import scala.reflect.ClassTag
import scopt.OptionParser

/**
* Case class LabeledByteImage represents an object of image in byte format with name and label
*
* @param data image byte data
* @param imageName image name
* @param label image label
**/
case class LabeledByteImage(data: Array[Byte], imageName: String, label: Float)

/**
* Case class LabeledDfPoint stores single labeled data frame information
*
* @param features extracted features after the transformers
* @param imageName image name
* @param label image label
**/
case class LabeledDfPoint(features: DenseVector, imageName: String, label: Float)

object infer_imagenet_max {

  def main(args: Array[String]): Unit = {

    LoggerFilter.redirectSparkInfoLogs()
    Logger.getLogger("com.intel.analytics.bigdl.example").setLevel(Level.INFO)

    case class Params(
      folder: String = "./",
      modelPath: String = "",
      duration: Int = 60,
      batchSize: Int = 0
    )

    val parser = new OptionParser[Params]("Infer ImageNet Max") {
      opt[String]('f', "folder")
        .text("location of test image data")
        .action((x, c) => c.copy(folder = x))
        .required()
      opt[String]('m', "modelPath")
        .text("model snapshot location")
        .action((x, c) => c.copy(modelPath = x))
        .required()
      opt[Int]('d', "duration")
        .text("duration (sec)")
        .action((x, c) => c.copy(duration = x))
      opt[Int]('b', "batchSize")
        .text("batch size (if omitted, defaults to number of executors)")
        .action((x, c) => c.copy(batchSize = x))
    }

    parser.parse(args, Params()).foreach { param => 
      val conf = Engine.createSparkConf()
      conf.setAppName("Predict with trained model")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      Engine.init
      val partitionNum = Engine.nodeNumber() * Engine.coreNumber()

      val transf = RowToByteRecords() ->
        BytesToBGRImg() ->
        BGRImgCropper(imageSize, imageSize) ->
        BGRImgNormalizer(testMean, testStd) ->
        BGRImgToImageVector()

      println("\n%s: Loading trained model from %s".format(Instant.now.toString, param.modelPath))
      val model = loadTorchModel(param.modelPath)

      val valTrans = new DLClassifierModel(model, Array(3, imageSize, imageSize))
        .setBatchSize(if (param.batchSize > 0) param.batchSize else Engine.nodeNumber())
        .setFeaturesCol("features")
        .setPredictionCol("predict")

   // Load image set from folders. Use each image 5 times to lower local image storage size and load time
      println("%s: Loading images from %s".format(Instant.now.toString, param.folder))
      val paths = readPaths1(Paths.get(param.folder))
      val images = imagesLoadLabeled(paths, 256)
      val images5 = Array.concat(images, images, images, images, images) 
      var n_images = images5.length
      println("%s: %d images found, using each image 5x for %d total images".format(Instant.now.toString, n_images/5, n_images))
      val valRDD =  sc.parallelize(images5, partitionNum)
      println("%s: Parallelization complete".format(Instant.now.toString))

      val valDF = transformLabeledDF(sqlContext.createDataFrame(valRDD), transf)

      println("%s: Running inference loop for %d seconds".format(Instant.now.toString, param.duration))
      val start_time = System.nanoTime
      var elapsed_time = 0.0D
      var loop_count = 1
      var total_images_classified = 0

      do {
        val vt = valTrans.transform(valDF)
        val vt2 = vt.withColumn("Correct", (col("label") === col("predict")).cast("Int"))
        val tot_correct  = vt2.agg(sum("Correct")).first.get(0)
        val finish_time = System.nanoTime
        elapsed_time = (finish_time - start_time)/1000000000.0 
        println("%s: Iteration %d: %d images inferred. %d or %.1f%% predicted correctly "
          .format(Instant.now.toString, loop_count, n_images, tot_correct, 100.0*tot_correct.asInstanceOf[Long]/n_images))
        loop_count += 1
        total_images_classified += n_images
      } while (elapsed_time < param.duration)

      println("%s: Test completion: %d images inferred in %.1f sec or %.1f images/second"
        .format(Instant.now.toString, total_images_classified, elapsed_time, total_images_classified.toFloat/elapsed_time))
      sc.stop()
    }
  }

  def loadTorchModel[@specialized(Float, Double) T : ClassTag](modelPath: String)
    (implicit ev: TensorNumeric[T]): Module[T] = { Module.loadTorch[T](modelPath) }

  def readPaths1(path: Path)
  : Array[LocalLabeledImagePath] = {
     readPathsWithLabelNoSort(path) 
  }

  def readPathsWithLabelNoSort(path: Path): Array[LocalLabeledImagePath] = {
    val directoryStream = Files.newDirectoryStream(path)
    val labelMap = readLabels(path)
    import scala.collection.JavaConverters._
    directoryStream.asScala.flatMap(dir => {
      //println(s"Find class ${dir.getFileName} -> ${labelMap(dir.getFileName.toString)}")
      Files.newDirectoryStream(dir).asScala.map(p =>
        LocalLabeledImagePath(labelMap(dir.getFileName.toString).toFloat, p)).toSeq
    }).toArray
  }

  def readLabels(path: Path): Map[String, Int] = {
    import scala.collection.JavaConverters._
    Files.newDirectoryStream(path).asScala.map(_.getFileName.toString)
      .toArray.sortWith(_ < _).zipWithIndex.map(c => c._1 -> (c._2 + 1)).toMap
  }

  def imagesLoadLabeled(paths: Array[LocalLabeledImagePath], scaleTo: Int):
    Array[LabeledByteImage] = {
    val buffer = paths.map(imageFile => {
    LabeledByteImage(BGRImage.readImage(imageFile.path, scaleTo), imageFile.path.getFileName.toString, imageFile.label)
    })
    buffer
  }

  def transformLabeledDF(data: DataFrame, f: Transformer[Row, DenseVector]): DataFrame = {
    val vectorRdd = data.select("data").rdd.mapPartitions(f(_))
    val name_labelRDD = data.rdd.map(_.getAs[String]("imageName")).zip(data.rdd.map(_.getAs[Float]("label")))
    val dataRDD = name_labelRDD.zipPartitions(vectorRdd) { (a, b) => b.zip(a)
      .map( v => LabeledDfPoint(v._1, v._2._1, v._2._2)) }
    data.sqlContext.createDataFrame(dataRDD)
  }
}
