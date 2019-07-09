"""
infer_cifar_max.py: read pre-labeled CIFAR10 images from memory, infer image classification using saved
  pre-trained BigDL Keras CNN model, compare to label, run for specified time

Usage:
  $ spark-submit <Spark config params> --jars <path>/bigdl-SPARK_2.4-0.8.0-jar-with-dependencies.jar infer_cifar_max.py <arguments>

  Arguments:
  -h          | --help                      print help message
  -md <value> | --modelDefsPath <value>     model definitions path     Required
  -mw <value> | --modelWeightsPath <value>  model weights path         Required
  -d  <value> | --duration <value>          duration (sec)             Default: 10

Uses Intel's BigDL library (see https://github.com/intel-analytics/BigDL-Tutorials) and CIFAR10 dataset from https://www.cs.toronto.edu/~kriz/cifar.html
(Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009, https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf)
Based on https://github.com/intel-analytics/BigDL/blob/master/pyspark/bigdl/examples/keras/mnist_cnn.py
Modified for CIFAR10 using convnet from https://github.com/keras-team/keras/blob/master/examples/cifar10_cnn.py, modified for Keras 1.2.2

Copyright (c) 2019 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

import sys
import argparse
import numpy as np
from time import time, gmtime, strftime, sleep

from bigdl.util.common import *
from bigdl.nn.layer import *
from bigdl.optim.optimizer import *
from keras.datasets import cifar10

from pyspark import SparkContext

parser = argparse.ArgumentParser(description='Infer CIFAR10 images using pre-trained BigDL Keras CNN model')
parser.add_argument("-md", "--modelDefsPath", type=str, dest="modelDefsPath", required=True)
parser.add_argument("-mw", "--modelWeightsPath", type=str, dest="modelWeightsPath", required=True)
parser.add_argument("-d", "--duration", type=int, dest="duration", default=10)
args = parser.parse_args()
model_defs_path=args.modelDefsPath
model_weights_path=args.modelWeightsPath
duration=args.duration

sc = SparkContext(appName="infer_cifar_max", conf=create_spark_conf())

# Download CIFAR10 train images or read from local cache
# Normalize and transform input data into an RDD of Sample
(train_images, train_labels), (test_images, test_labels) = cifar10.load_data()
num_classes = len(np.unique(train_labels))
num_train_images = train_images.shape[0]
training_mean = np.mean(train_images)
training_std = np.std(train_images)
rdd_train_images = sc.parallelize(train_images)
rdd_train_labels = sc.parallelize(train_labels)
rdd_train_sample  = rdd_train_images.zip(rdd_train_labels).map(lambda il_tuple:
        Sample.from_ndarray((il_tuple[0] - training_mean)/training_std, il_tuple[1] + 1))

# Start BigDL
init_engine()
redire_spark_logs()
show_bigdl_info_logs()

# Load model
print("\nLoading trained model from BDL_KERAS_CIFAR_CNN.bigdl.8 (definition) and BDL_KERAS_CIFAR_CNN.bin.8 (weights)")
model = Model.loadModel(model_defs_path, model_weights_path)

# Run train set through prediction loop for specified time
print('%s.%03dZ: Running inference loop for %d seconds' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, duration))
iteration = 0
total_images_predicted = 0
start_time = time()
while True:
  rdd_predictions = model.predict_class(rdd_train_sample)
  rdd_preds_labels = rdd_predictions.zip(rdd_train_labels.map(lambda a: a[0] + 1))
  correct_preds = rdd_preds_labels.map(lambda preds_labels: preds_labels[0] == preds_labels[1]).reduce(lambda accum,n: int(accum)+int(n))
  iteration += 1
  total_images_predicted += num_train_images
  print('%s.%03dZ: Iteration %d: %d images inferred. Correct predictions: %d  Pct correct: %.4f' % 
   (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, iteration, num_train_images, correct_preds, float(correct_preds)/num_train_images))
  if (time() - start_time) > duration:
    break

elapsed_time = time() - start_time
print('%s.%03dZ: Test completion: %d images inferred in %.1f sec or %.1f images/second' % 
   (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, total_images_predicted, elapsed_time, total_images_predicted/elapsed_time))

sc.stop()
