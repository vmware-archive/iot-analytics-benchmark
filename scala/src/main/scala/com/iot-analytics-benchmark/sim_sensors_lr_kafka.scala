/*
sim_sensors_lr_kafka.scala: Program to generate sensor data for Spark streaming logistic regression model prediction and send to Kafka
Usage: scala -cp <path>iotstream_<scala version>-<code version>.jar:/root/kafka/libs/* com.iotstream.sim_sensors_lr_kafka n_sensors average_sensor_events_per_second total_events kafka_server_list kafka_topic
kafka_server_list is comma-separated list of IPaddr:port, eg "192.168.1.2:9092,192.168.2.2:9092,192.168.3.2:9092" 

Copyright (c) 2018 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package com.iotstream

object sim_sensors_lr_kafka extends App {

  import java.util.Properties
  import org.apache.kafka.clients.producer._
  import java.time.Instant
  import java.util.Random
  import scala.math._

  if (args.length != 5) {
    System.err.println("Usage: scala -cp <path>iotstream_<scala version>-<code version>.jar:/root/kafka/libs/* com.iotstream.sim_sensors_lr_kafka n_sensors average_sensor_events_per_second total_events kafka_server_list kafka_topic")
    System.exit(-1)
  }

  val n_sensors = args(0).toInt
  val events_per_second = args(1).toInt
  val total_events = args(2).toInt
  val kafka_server_list = args(3)
  val topic = args(4)

  println(Instant.now() + ": Sending " + events_per_second + " sensor events per second representing " + n_sensors + " sensors for a total of " + total_events + " events to Kafka topic " + topic + " using Kafka server(s) " + kafka_server_list)
     
  val  props = new Properties()
  props.put("bootstrap.servers", kafka_server_list)
  props.put("acks","1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("buffer.memory", "1073741824")  // 1GiB, adjust as necessary
  props.put("batch.size", "100000")
   
  val producer = new KafkaProducer[String, String](props)
   
  def simulateSensorEvent(sensor_number: Int, value: Double): Unit = {
    // Output format:  Timestamp (string), sensor number (integer), sensor name (string), sensor value (float), eg 2017-12-14T22:22:43.895Z,19,Sensor 19,0.94764
    val record = new ProducerRecord(topic, "key", "%s,%d,Sensor %d,%.5f".format(Instant.now().toString(), sensor_number, sensor_number, value))
    producer.send(record)
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

  producer.close()
  println(Instant.now() + ": Sent " + total_events + " events to Kafka topic " + topic + " in " + "%.1f".format((t2-t1)/1000000000.0) + " seconds")
}
