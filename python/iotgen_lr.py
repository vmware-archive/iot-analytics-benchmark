#! /usr/bin/python
"""
iotgen_lr.py: Spark program to generate CSV sensor data for training logistic regression model
  Breaks computation into specified number of partitions, each partition's data is converted to CSV and then written to S3
  Stores output file on HDFS or S3
Usage: spark-submit iotgen_lr.py n_rows n_sensors n_partitions HDFS_or_S3 HDFS_path_or_S3_bucket filename <cutoff>
For exact number of rows make n_rows integer multiple of n_partitions
If cutoff not specified will generate 50% 1 labels - use calc_cutoffs.py to calculate other percentages

Copyright (c) 2018 VMware, Inc.
 
This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.
 
This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

import sys, random, math
import numpy as np
from time import time, gmtime, strftime
from pyspark import SparkConf, SparkContext

if not (len(sys.argv) == 7 or len(sys.argv) == 8):
  print >> sys.stderr, "Usage: spark-submit iotgen_lr.py n_rows n_sensors n_partitions HDFS_or_S3 HDFS_path_or_S3_bucket filename <cutoff>"
  exit(-1)

n_rows    = int(sys.argv[1])
n_sensors = int(sys.argv[2])
n_partitions  = int(sys.argv[3])
if sys.argv[4].capitalize() == 'S3':
  ofilename = "s3a://{}/{}".format(sys.argv[5], sys.argv[6])
else:
  ofilename = "{}/{}".format(sys.argv[5], sys.argv[6])

if len(sys.argv) == 8:
  cutoff = float(sys.argv[7])
else:
  cutoff = .25 * n_sensors * (n_sensors+1)

partition_size = int(math.ceil(float(n_rows)/float(n_partitions)))  # In case n_rows not integer multiple of n_partitions
n_rows = partition_size * n_partitions

print "%sZ: Creating file %s with %d rows of %d sensors, each row preceded by score using cutoff %.1f, in %d partitions" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), ofilename, n_rows, n_sensors, cutoff, n_partitions)

start_time = time()

def create_sensor_data_partition(i_partition):
  sensor_array = np.zeros((partition_size, n_sensors+1))
  for i in range(partition_size):
    sensors = []
    # Assign a random number between 0 and 1 to each sensor value
    for s in range (0, n_sensors):
      sensors.append(random.random())
    # Weight score by sensor number
    score = 0
    for s in range (0, n_sensors):
      score += sensors[s]*(s+1)
    # Assign a label 
    label =  [0,1] [score > cutoff]
    sensors.insert(0, label) 
    sensor_array[i] = sensors
  return sensor_array

def toCSVLine(data):
  return ','.join('%.5f'% d for d in data)

sc = SparkContext(appName="iotgen_lr")

# Create an RDD with n_partitions elements, send each to create_sensor_data_partition, combine results, convert to CSV output and save to ofilename
r = sc.parallelize(range(n_partitions), n_partitions)
lines = r.map(create_sensor_data_partition).flatMap(lambda a: a.tolist()).map(toCSVLine)
lines.saveAsTextFile(ofilename)

elapsed_time = time() - start_time
size = float((n_sensors+1)*8*n_rows)
KiB,MiB,GiB,TiB = pow(2,10),pow(2,20),pow(2,30),pow(2,40)
if (size >= TiB):
  size_str = "%.1fTB" % (size/TiB) 
elif (size >= GiB):
  size_str = "%.1fGB" % (size/GiB) 
elif (size >= MiB):
  size_str = "%.1fMB" % (size/MiB) 
elif (size >= KiB):
  size_str = "%.1fKB" % (size/KiB) 
else:
  size_str = "%d" % size
print "%sZ: Created file %s with size %s in %.1f seconds" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), ofilename, size_str, elapsed_time)
