#! /usr/bin/python
"""
iotgen_lr_python.py: Python program to generate CSV sensor data for training logistic regression model
Usage: python iotgen_lr.py n_rows n_sensors csv_output_filename <cutoff>
  If cutoff not specified will generate 50% 1 labels - use calc_cutoffs.py to calculate other percentages

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""
from __future__ import print_function

import sys, csv, random

if not (len(sys.argv) == 4 or len(sys.argv) == 5):
  print("Usage: python iotgen_lr.py n_rows n_sensors csv_output_filename <cutoff>", file=sys.stderr)
  exit(-1)

n_rows = int(sys.argv[1])
n_sensors = int(sys.argv[2])
ofilename = sys.argv[3]
outfile = open(ofilename, 'w')
if len(sys.argv) == 5:
  cutoff = int(sys.argv[4])
else:
  cutoff = int(.25 * n_sensors * (n_sensors+1))

print("Creating file {} with {} rows of {} sensors, each row preceded by score using cutoff {}".format(ofilename, n_rows, n_sensors, cutoff))

def toCSVLine(data):
  return ','.join('%.5f'% d for d in data)

for i in range(0, n_rows):
  sensors = []
  # Assign a random number between 0 and 1 to each sensor value
  for s in range (0, n_sensors):
    sensors.append(random.random())
  # Weight score by sensor number
  score = 0
  for s in range (0, n_sensors):
    score += sensors[s]*(s+1)
  # Assign a label 
  label = [0,1] [score > cutoff]
  sensors.insert(0, label) 
  outfile.write(toCSVLine(sensors) + '\n')

outfile.close()

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
print("Created file {} with size {}".format(ofilename, size_str))
