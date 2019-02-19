#! /usr/bin/python
"""
iottrain_lr.py: Spark program to train and generate logistic regression model from pre-collected sensor data
  Reads training data and stores model on HDFS or S3
Usage: spark-submit iottrain_lr.py HDFS_or_S3 HDFS_path_or_S3_bucket filename modelname

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

from __future__ import print_function

import os, sys
from time import gmtime, strftime
from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

if len(sys.argv) != 5:
  print("Usage: spark-submit iottrain_lr.py HDFS_or_S3 HDFS_path_or_S3_bucket filename modelname", file=sys.stderr)
  exit(-1)

if sys.argv[1].capitalize() == 'S3':
  ifilename = "s3a://{}/{}".format(sys.argv[2], sys.argv[3])
  ofilename = "s3a://{}/{}".format(sys.argv[2], sys.argv[4])
else:
  ifilename = "{}/{}".format(sys.argv[2], sys.argv[3])
  ofilename = "{}/{}".format(sys.argv[2], sys.argv[4])

print("%sZ: Training logistic regression model and storing as %s using data from %s" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), ofilename, ifilename))

# Load and parse the data
def parsePoint(line):
  values = [float(x) for x in line.split(',')]
  return LabeledPoint(values[0], values[1:])

sc = SparkContext(appName="iottrain_lr")

data = sc.textFile(ifilename)
parsedData = data.map(parsePoint)

# Build the model
model = LogisticRegressionWithLBFGS.train(parsedData)

# Save model
print("%sZ: Trained logistic regression model and storing as %s" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), ofilename))
model.save(sc, ofilename)

print("%sZ: Trained logistic regression model and stored as %s" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), ofilename))
