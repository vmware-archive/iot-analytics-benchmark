/*
sim_sensors_lr_mqtt.scala: Program to generate sensor data for Spark streaming logistic regression model prediction and send to MQTT topic
Usage: scala -cp <path>iotstream_<scala version>-<code version>.jar:<path>org.eclipse.paho.client.mqttv3-1.2.0.jar com.iotstream.sim_sensors_lr_mqtt n_sensors average_sensor_events_per_second total_events mqtt_server mqtt_port mqtt_topic

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package com.iotstream

object sim_sensors_lr_mqtt extends App {

  import java.util.Properties
  import java.time.Instant
  import java.util.Random
  import scala.math._
  import org.eclipse.paho.client.mqttv3._
  import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
  
  if (args.length != 6) {
    System.err.println("Usage: scala -cp <path>iotstream_<scala version>-<code version>.jar:<path>org.eclipse.paho.client.mqttv3-1.2.0.jar com.iotstream.sim_sensors_lr_mqtt n_sensors average_sensor_events_per_second total_events mqtt_server mqtt_port mqtt_topic")
    System.exit(-1)
  }

  val n_sensors = args(0).toInt
  val events_per_second = args(1).toInt
  val total_events = args(2).toInt
  val mqtt_URL = "tcp://%s:%s".format(args(3), args(4))
  val topic = args(5)

  println(Instant.now() + ": Sending " + events_per_second + " sensor events per second representing " + n_sensors + " sensors for a total of " + total_events + " events to MQTT topic " + topic + " using MQTT URL " + mqtt_URL)

  val persistence = new MemoryPersistence()
  val client = new MqttClient(mqtt_URL, MqttClient.generateClientId(), persistence)
  client.connect()
  val msgtopic = client.getTopic(topic)
     
  def simulateSensorEvent(sensor_number: Int, value: Double): Unit = {
    // Output format:  Timestamp (string), sensor number (integer), sensor name (string), sensor value (float), eg 2017-12-14T22:22:43.895Z,19,Sensor 19,0.94764
    val msgContent = "%s,%d,Sensor %d,%.5f".format(Instant.now().toString(), sensor_number, sensor_number, value)
    val message = new MqttMessage(msgContent.getBytes("utf-8"))
    msgtopic.publish(message)
  }

  def accurateWait(wait_in_seconds: Double): Unit = {
    val  waitUntil = System.nanoTime() + round(1000000000 * wait_in_seconds)
    while (waitUntil > System.nanoTime()) {}
  }

  val rand = new Random
  // Use lognormal distribution to generate a positive random wait time with mean determined from events_per_second and long tail
  val mean_wait = 1.0/events_per_second
  // Set standard deviation to half the mean_wait
  val std_dev = mean_wait/2.0

  val t1 = System.nanoTime

  for (i <- 1 to total_events) {
    // Assign a random number between 1 and n_sensors to sensor index and between 0 and 1 to sensor value
    simulateSensorEvent(rand.nextInt(n_sensors) + 1, rand.nextDouble())
    if (i % events_per_second == 0) println(Instant.now() + ": " + i + " events sent")
    // Wait a random time from lognormal distribution with mean mean_wait
    accurateWait(mean_wait * exp(rand.nextGaussian()*std_dev + mean_wait)/exp(mean_wait + pow(std_dev,2)/2))
  }

  val t2 = System.nanoTime

  //  Send negative sensor number to indicate end of stream
  simulateSensorEvent(-1,0)

  client.disconnect()
  println(Instant.now() + ": Sent " + total_events + " events to MQTT topic " + topic + " in " + "%.1f".format((t2-t1)/1000000000.0) + " seconds")
  System.exit(0)
}
