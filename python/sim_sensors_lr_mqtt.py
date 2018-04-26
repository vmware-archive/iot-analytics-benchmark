#! /usr/bin/python
"""
sim_sensors_lr_mqtt.py: Program to generate sensor data for Spark streaming logistic regression model prediction and send to MQTT topic
Usage: python sim_sensors_lr_mqtt.py n_sensors average_sensor_events_per_second total_events mqtt_server mqtt_port mqtt_topic

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

import sys, random
from time import time, gmtime, strftime, sleep
from math import exp
import paho.mqtt.client as mqtt

if len(sys.argv) != 7:
    print >> sys.stderr, "Usage: python sim_sensors_lr_mqtt.py n_sensors average_sensor_events_per_second total_events mqtt_server mqtt_port mqtt_topic"
    exit(-1)

n_sensors = int(sys.argv[1])
events_per_second = int(sys.argv[2])
total_events = int(sys.argv[3])
mqtt_server = sys.argv[4]
mqtt_port = sys.argv[5]
mqtt_topic = sys.argv[6]

print "Generating {} sensor events per second representing {} sensors for a total of {} events and sending to MQTT topic {} using MQTT server {}".format(events_per_second, n_sensors, total_events, mqtt_topic, mqtt_server)

def simulateSensorEvent(i, sensor_number, value):
# Output format:  Timestamp (string), sensor number (integer), sensor name (string), sensor value (float), eg
# 2017-12-14T22:22:43.895Z,19,Sensor 19,0.947640
  client.publish(mqtt_topic, '%s.%03dZ,%i,Sensor %i,%.5f' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, sensor_number, sensor_number, value))
 
client = mqtt.Client()
client.connect(mqtt_server, mqtt_port, 60)

# Use lognormal distribution to generate a positive random wait time with mean determined from events_per_second and long tail
mean_wait = float(1.0/events_per_second)
# Set standard deviation to half the mean_wait
std_dev = mean_wait/2.0

start_time = time()

for i in range(1, total_events+1):
  # Assign a random number between 1 and n_sensors to sensor index and between 0 and 1 to sensor value
  simulateSensorEvent(i, random.randint(1,n_sensors), random.uniform(0.0,1.0))
  if i % events_per_second == 0:
    print '%s.%03dZ: %d events sent' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, i)
  # Wait a random time from lognormal distribution with mean mean_wait
  sleep(mean_wait*random.lognormvariate(mean_wait,std_dev)/exp(mean_wait + std_dev**2/2))
  

# Send negative sensor number to indicate end of stream
simulateSensorEvent(i+1,-1,0)
finish_time = time()
print '%s.%03dZ: Sent %d events to MQTT topic %s in %.1f seconds' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, total_events, mqtt_topic, finish_time-start_time)
