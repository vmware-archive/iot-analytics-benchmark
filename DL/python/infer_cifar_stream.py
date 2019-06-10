"""
infer_cifar_stream.py: read pre-labeled CIFAR10 images using Spark Streaming, infer object using saved pre-trained model, compare to label

In one window:
  $ python3 send_images_cifar_stream.py | nc -lk port_of_image_source

In another window:
  $ spark-submit <Spark config params> --jars <path>/bigdl-SPARK_2.3-0.7.0-jar-with-dependencies.jar infer_cifar_stream.py <arguments>

  Arguments:
  -h          | --help                      print help message
  -md <value> | --modelDefsPath <value>     model definitions path     Required
  -mw <value> | --modelWeightsPath <value>  model weights path         Required
  -r  <value> | --reportingInterval <value> reporting interval (sec)   Default: 10
  -i  <value> | --sourceIPAddress <value>   source IP address          Default: 192.168.1.1
  -p  <value> | --sourcePort <value>        source port                Default: 10000

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
from io import StringIO
from time import time, gmtime, strftime, sleep

from bigdl.nn.layer import *
from bigdl.util.common import *

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

parser = argparse.ArgumentParser(description='Infer CIFAR10 images using pre-trained BigDL Keras CNN model')
parser.add_argument("-md", "--modelDefsPath", type=str, dest="modelDefsPath", required=True)
parser.add_argument("-mw", "--modelWeightsPath", type=str, dest="modelWeightsPath", required=True)
parser.add_argument("-r", "--reportingInterval", type=int, dest="reportingInterval", default=10)
parser.add_argument("-i", "--sourceIPAddress", type=str, dest="sourceIPAddress", default="192.168.1.1")
parser.add_argument("-p", "--sourcePort", type=int, dest="sourcePort", default=10000)
args = parser.parse_args()
model_defs_path=args.modelDefsPath
model_weights_path=args.modelWeightsPath
reporting_interval=args.reportingInterval
IP_address=args.sourceIPAddress
port=args.sourcePort

def parse_labeled_image_strings(rdd_labeled_image_strings):
  rdd_labels = rdd_labeled_image_strings.map(lambda string: np.uint8(string[0]))
  rdd_images = rdd_labeled_image_strings.map(lambda string: np.loadtxt(StringIO(string[1:]), dtype=int).reshape((32,32,3)))
  return(rdd_images, rdd_labels)

def run_model(rdd):
  if rdd.count() == 0:
    empty_intervals.add(1)
    print('%s.%03dZ: No images received in interval' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000))
    if interval.value > 0:
      print('%s.%03dZ: Stopping stream' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000))
      ssc.stop()
  else:
    interval.add(1)
    input_length = rdd.count()
    images.add(input_length)
    rdd_test_images, rdd_test_labels = parse_labeled_image_strings(rdd)
    rdd_test_sample  = rdd_test_images.map(lambda image: Sample.from_ndarray((image - training_mean)/training_std, 0))
    predictions = model.predict_class(rdd_test_sample).collect()
    labels = rdd_test_labels.collect()
    correct_preds  = 0
    for i in range(input_length):
      if (predictions[i]-1 == labels[i]): # predict_class returns index of predicted class starting at 1
       correct_preds += 1
    correct_preds_tot.add(correct_preds)
    print('%s.%03dZ: Interval %d:  images received=%d   images correctly predicted=%d' %  
      (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, interval.value, input_length, correct_preds))

# From CIFAR10 Dataset
training_mean = 120.70756512369792
training_std = 64.1500758911213

# Initialize SparkContext, BigDL engine and various accumulators
sc = SparkContext(appName="infer_cifar_stream", conf=create_spark_conf())
init_engine()
interval = sc.accumulator(0)
empty_intervals = sc.accumulator(0)
images  = sc.accumulator(0)
correct_preds_tot  = sc.accumulator(0)

# Load model trained using BDL_KERAS_CIFAR_CNN.py
model = Model.loadModel(model_defs_path, model_weights_path)
print('%s.%03dZ: Loaded trained model definitions %s and weights %s' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, model_defs_path, model_weights_path))
print('%s.%03dZ: Starting reading streaming data from %s:%d at interval %s seconds' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, IP_address, port, reporting_interval))

# Initialize StreamingContext, have it read TextStream through socket
ssc = StreamingContext(sc, reporting_interval)
image_stream = ssc.socketTextStream(IP_address, port)

# Run model on each batch
image_stream.foreachRDD(run_model)

# Start reading streaming data
ssc.start()
start_time = time()
ssc.awaitTermination()
elapsed_time = time() - start_time - empty_intervals.value*reporting_interval - 2.4 # Subtract empty intervals and time to shut down stream
print('\n%s.%03dZ: %d images received in %.1f seconds (%d intervals), or %.0f images/second  Correct predictions: %d  Pct correct: %.1f' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, images.value,  elapsed_time, interval.value, float(images.value)/elapsed_time, correct_preds_tot.value, float(100*correct_preds_tot.value)/images.value))

sc.stop()
