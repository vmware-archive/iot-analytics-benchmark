#! /usr/bin/python
"""
sim_sensors_lr.py: Program to generate sensor data for Spark streaming logistic regression model prediction
Usage: python sim_sensors_lr.py n_sensors average_sensor_events_per_second total_events

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

import sys, random
from time import time, gmtime, strftime, sleep
from math import exp

if len(sys.argv) != 4:
    print >> sys.stderr, "Usage: python sim_sensors_lr.py n_sensors average_sensor_events_per_second total_events"
    exit(-1)

n_sensors = int(sys.argv[1])
events_per_second = int(sys.argv[2])
total_events = int(sys.argv[3])

print >> sys.stderr, "%sZ: Generating %d sensor events per second representing %d sensors for a total of %d events" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), events_per_second, n_sensors, total_events)


def simulateSensorEvent(sensor_number, value):
# Output format:  Timestamp (string), sensor number (integer), sensor name (string), sensor value (float), eg
# 2017-12-14T22:22:43.895Z,19,Sensor 19,0.947640
  print '%s.%03dZ,%i,Sensor %i,%.5f' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, sensor_number, sensor_number, value)
  sys.stdout.flush()

for i in range(total_events):
  # Assign a random number between 0 and 1 to sensor value
  value = random.random()
  simulateSensorEvent(random.randint(1,n_sensors), value)
  # Use lognormal distribution to generate a positive random wait time with mean determined from events_per_second and long tail
  mean_wait = float(1.0/events_per_second)
  # Set standard deviation to half the mean_wait
  std_dev = mean_wait/2.0
  sleep(mean_wait*random.lognormvariate(mean_wait,std_dev)/exp(mean_wait + std_dev**2/2))
  if ((i+1) % events_per_second == 0): 
    print >> sys.stderr, "%sZ: %d events sent" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), i+1)

# Send negative sensor number to indicate end of stream
simulateSensorEvent(-1,0)

print >> sys.stderr, "%sZ: Sensor stream ended - keeping socket open for 1000 seconds" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()))

# Add 1000 seconds at end to keep socket open while Spark streaming completes
sleep(1000)
