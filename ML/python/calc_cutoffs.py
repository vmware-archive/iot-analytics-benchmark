#! /usr/bin/python
"""
calc_cutoffs.py: Utility program to calculate sensor data scores cutoff for a given number of sensors
Usage: python calc_cutoffs.py n_rows n_sensors

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""
from __future__ import print_function

import sys, random

if len(sys.argv) != 3:
  print("Usage: python calc_cutoffs.py n_rows n_sensors", file=sys.stderr)
  exit(-1)

n_rows = int(sys.argv[1])
n_sensors = int(sys.argv[2])

print("Cutoffs for {} rows of {} sensors".format(n_rows, n_sensors))

scores = []
for i in range(0, n_rows):
  sensors = []
  # Assign a random number between 0 and 1 to each sensor value
  for s in range (0, n_sensors):
    sensors.append(random.random())
  # Weight score by sensor number
  score = 0
  for s in range (0, n_sensors):
    score += sensors[s]*(s+1)
  scores.append(score) 
# Sort scores and print 100 cutoffs
scores.sort()
cutoffs = scores[int(n_rows/100)::int(n_rows/100)]
for p in range(99):
  print('%d %.0f' % (99-p, cutoffs[p]))
