

# iot-analytics-benchmark DL

## Introduction

IoT Analytics Benchmark DL consists of neural network-based Deep Learning image classification programs run on a stream of images.

The programs run Keras and BigDL image classifiers using pre-trained models and the CIFAR10 image set. For each type of classifier there is a program that sends the images
as a series of encoded strings and a second program that reads those string, converts them back to images, and infers which of the 10 CIFAR10 classes that image belongs to.

The Keras classifier is a Python-based single node program for running on an IoT edge gateway.

The BigDL classifiers (Python and Scala versions) are Spark-based distributed programs using Spark Streaming to read and infer the input encoded images.

Uses Intel's BigDL library (see <https://github.com/intel-analytics/BigDL-Tutorials>) and  
CIFAR10 dataset from <https://www.cs.toronto.edu/~kriz/cifar.html>   
See [Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009](https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf)

## Installation

- Install Spark 
  - Spark single node installation: obtain latest version from <http://spark.apache.org/downloads.html> and unzip
  - Spark release 2.4.0, using package "Prebuilt for Apache Hadoop 2.7 and later", tested here

- Install python3 and pip3 on all nodes, add numpy, keras and tensorflow with pip3  

  Example on Centos 7:
  ```
  $ yum install https://centos7.iuscommunity.org/ius-release.rpm
  $ yum install python36u python36u-pip
  $ ln -s /usr/bin/python3.6 /usr/bin/python3
  $ ln -s /usr/bin/pip3.6 /usr/bin/pip3
  $ pip3 install --upgrade pip
  $ pip3 install numpy keras ipython tensorflow
  ```

- Install nc on driver node (`yum install nc`)

- For purposes of this documentation, a symbolic link to the Spark code on the driver system is assumed. For example:
  `ln -s /root/spark-2.4.0-bin-hadoop2.7 /root/spark`

- Add spark/bin directory to `$PATH`

- Set log level from INFO to WARN or ERROR or OFF (suggested for cleaner output, especially of Spark Streaming output, which can show errors upon stream end):

  In `spark/conf`:  
  `cp log4j.properties.template log4j.properties`  
  edit `log4j.properties`:  
  `log4j.rootCategory=ERROR, console`  

- Clone or download and unzip project

## Project Files

File                                  | Use
:----                                 | :---
`infer_cifar.py`                      | Python Keras program to classify CIFAR10 images using CNN or ResNet model
`send_images_cifar.py`                | Send images to infer_cifar.py
`keras_cifar10_trained_model_78.h5`   | Trained CNN model for Python Keras program - 78% accurate
`cifar10_ResNet20v1_model_91470.h5`   | Trained ResNet model for Python Keras program - 91.4% accurate
`infer_cifar_stream.py`               | Spark Streaming BigDL program to classify CIFAR10 images using CNN model
`send_images_cifar_stream.py`         | Send images to infer_cifar_stream.py
`BDL_KERAS_CIFAR_CNN.bigdl.8`         | Trained CNN model definition file for BigDL program - 80% accurate
`BDL_KERAS_CIFAR_CNN.bin.8`           | Trained CNN model weights file for BigDL program - 80% accurate
`infer_cifar_stream.scala`            | Spark Streaming BigDL scala program to classify CIFAR10 images using ResNet model
`send_images_cifar_stream.scala`      | Send images to infer_cifar_stream.scala
`bigdl_resnet_model_893`              | Trained ResNet model for Scala BigDL program - 89.3% accurate
`README.md`                           | This file


## Program usage (run any program with -h flag to see parameters)

### Python Keras CNN/ResNet CIFAR10 image classifier

In one shell, `cd <path>/iot-analytics-benchmark-master/DL/python`, then:

`nc -lk <port> | python3 infer_cifar.py [-h] -m MODELPATH [-r REPORTINGINTERVAL]`

Parameter          | Use
:---------         | :---
MODELPATH          | Location of trained model file - required
REPORTINGINTERVAL  | Reporting interval - defaults to every 100 images sent

Wait for program to output "Start send program" then, in a second shell in the same directory:

`python3 send_images_cifar.py [-h] [-s] [-i IMAGESPERSEC] [-t TOTALIMAGES] | nc <dest IP address>  <dest port>`

Parameter      | Use
:---------     | :---
IMAGESPERSEC   | Images per second to send - defaults to 10
TOTALIMAGES    | Total number of images to send - defaults to 100

Specify -s to subtract image mean from each image value - use for ResNet model

Example

```
$ nc -lk 10000 | python3 infer_cifar.py --modelPath cifar10_ResNet20v1_model_91470.h5 --reportingInterval 1000
Using TensorFlow backend.
Loaded trained model cifar10_ResNet20v1_model_91470.h5
Start send program
2019-01-31T02:44:45Z: 1000 images classified
...
2019-01-31T02:45:38Z: 10000 images classified
Inferenced 10000 images in 58.8 seconds or 170.0 images/second, with 9147 or 91.5% correctly classified

$ python3 send_images_cifar.py -s -i 1000 -t 10000 | nc 192.168.1.1 10000
Using TensorFlow backend.
2019-01-31T02:44:28Z: Loading and normalizing the CIFAR10 data
2019-01-31T02:44:39Z: Sending 1000 images per second for a total of 10000 images with pixel mean subtracted
2019-01-31T02:44:44Z: 1000 images sent
...
2019-01-31T02:45:37Z: 10000 images sent
2019-01-31T02:45:37Z: Image stream ended
```

### Spark Streaming BigDL CNN CIFAR10 image classifier

In one shell, `cd <path>/iot-analytics-benchmark-master/DL/python`, then:

`python3 send_images_cifar_stream.py [-h] [-i IMAGESPERSEC] [-t TOTALIMAGES] | nc -lk <port>`

Parameter      | Use
:---------     | :---
IMAGESPERSEC   | Images per second to send - defaults to 10
TOTALIMAGES    | Total number of images to send - defaults to 100

Specify -s to subtract image mean from each image value - use for ResNet model

Wait for "Pausing 15 seconds - start infer_cifar_stream.py", then in a second shell:

```
export SPARK_HOME=/root/spark
export PYSPARK_PYTHON=python3
spark-submit <Spark config params> --jars <path>/bigdl-SPARK_2.3-0.7.0-jar-with-dependencies.jar infer_cifar_stream.py \  
  [-h] -md MODELDEFSPATH -mw MODELWEIGHTSPATH [-r REPORTINGINTERVAL] [-i SOURCEIPADDRESS] [-p SOURCEPORT]
```

Parameter          | Use
:---------         | :---
MODELDEFSPATH      | Location of trained model definitions file - required
MODELWEIGHTSPATH   | Location of trained model weights file - required
REPORTINGINTERVAL  | Reporting interval - defaults to every 10 seconds
SOURCEIPADDRESS    | Source IP Address - defaults to 192.168.1.1
SOURCEPORT         | Source port - defaults to 10000

Example

```
$ python3 send_images_cifar_stream.py -i 12000 -t 1000000 | nc -lk 10000
Using TensorFlow backend.
2019-01-31T15:54:25Z: Loading and normalizing the CIFAR10 data
2019-01-31T15:54:34Z: Pausing 15 seconds - start infer_cifar_stream.py
2019-01-31T15:54:49Z: Sending 12000 images per second for a total of 1000000 images
2019-01-31T15:54:50Z: 12000 images sent
...
2019-01-31T15:56:14Z: 1000000 images sent
2019-01-31T15:56:14Z: Image stream ended - keeping socket open for 120 seconds

$ spark-submit --master spark://<host>:7077 --driver-memory 128G --conf spark.cores.max=250 --conf spark.executor.cores=10 \
--executor-memory 104g --jars <path>/BigDL/lib/bigdl-SPARK_2.3-0.7.0-jar-with-dependencies.jar infer_cifar_stream.py \
--modelDefsPath BDL_KERAS_CIFAR_CNN.bigdl.8 --modelWeightsPath BDL_KERAS_CIFAR_CNN.bin.8 -r 25
Prepending /usr/lib/python3.6/site-packages/bigdl/share/conf/spark-bigdl.conf to sys.path
Using TensorFlow backend.
cls.getname: com.intel.analytics.bigdl.python.api.Sample
BigDLBasePickler registering: bigdl.util.common  Sample
cls.getname: com.intel.analytics.bigdl.python.api.EvaluatedResult
BigDLBasePickler registering: bigdl.util.common  EvaluatedResult
cls.getname: com.intel.analytics.bigdl.python.api.JTensor
BigDLBasePickler registering: bigdl.util.common  JTensor
cls.getname: com.intel.analytics.bigdl.python.api.JActivity
BigDLBasePickler registering: bigdl.util.common  JActivity
2019-01-31T15:54:40.199Z: Loaded trained model definitions BDL_KERAS_CIFAR_CNN.bigdl.8 and weights BDL_KERAS_CIFAR_CNN.bin.8
2019-01-31T15:54:40.199Z: Starting reading streaming data from 192.168.1.1:10000 at interval 25 seconds
2019-01-31T15:55:14.059Z: Interval 1:  images received=126280   images correctly predicted=101124
2019-01-31T15:55:45.381Z: Interval 2:  images received=295772   images correctly predicted=236811
2019-01-31T15:56:10.629Z: Interval 3:  images received=284603   images correctly predicted=227911
2019-01-31T15:56:33.227Z: Interval 4:  images received=292563   images correctly predicted=234241
2019-01-31T15:56:43.345Z: Interval 5:  images received=782   images correctly predicted=613
2019-01-31T15:57:05.017Z: Stopping stream

2019-01-31T15:57:07.422Z: 1000000 images received in 116.0 seconds (5 intervals), or 8619 images/second  Correct predictions: 800700  Pct correct: 80.1
```

### Spark Streaming BigDL ResNet CIFAR10 Scala image classifier

In one shell:

```
java -Xmx128g -cp <path>/scala-library.jar:<path>/hadoop-common-3.0.0.jar:<path>/bigdl-SPARK_2.3-0.7.0-jar-with-dependencies.jar:<path>/iotstreamdl_2.11-0.0.1.jar \
  com.intel.analytics.bigdl.models.resnet.send_images_cifar_stream <arguments> | nc -lk <port>
```

Arguments:
```
  -f, --folder <value>        the location of Cifar10 dataset  Default: datasets/cifar-10-batches-bin
  -i, --imagesPerSec <value>  images per second                Default: 10
  -t, --totalImages <value>   total images                     Default: 100
```

Wait for "Pausing 15 seconds - start infer_cifar_stream", then in a second shell:

```
spark-submit <Spark config params> --jars <path>/bigdl-SPARK_2.3-0.7.0-jar-with-dependencies.jar \
  --class com.intel.analytics.bigdl.models.resnet.infer_cifar_stream <path>/iotstreamdl_2.11-0.0.1.jar <arguments>
```

Arguments:
```
  -r <value> | --reportingInterval <value> reporting interval (sec)   Default: 1
  -i <value> | --sourceIPAddress <value>   source IP address          Default: 192.168.1.1
  -p <value> | --sourcePort <value>        source port                Default: 10000
  -m <value> | --model <value>             model                      Required
  -b <value> | --batchSize <value>         batch size                 Default: 2000
```

Example

```
java -Xmx128g -cp <path>/scala-library.jar:<path>/hadoop-common-3.0.0.jar:<path>/bigdl-SPARK_2.3-0.7.0-jar-with-dependencies.jar:<path>/iotstreamdl_2.11-0.0.1.jar \
com.intel.analytics.bigdl.models.resnet.send_images_cifar_stream -i 12000 -t 1000000 | nc -lk 10000
Will send 12000 images per second for a total of 1000000 images
Pausing 15 seconds - start image_cifar_stream
2019-02-15T23:33:01.873Z: Sending images
2019-02-15T23:36:13.264Z: 12000 images sent
...
2019-02-15T23:38:03.182Z: Sent 1000000 images in 301.3 seconds


spark-submit --master spark://<host>:7077 --driver-memory 128G --conf spark.cores.max=250 --conf spark.executor.cores=10 \
--executor-memory 104g --jars ~/BigDL/lib/bigdl-SPARK_2.3-0.7.0-jar-with-dependencies.jar \
--class com.intel.analytics.bigdl.models.resnet.infer_cifar_stream iotstreamdl_2.11-0.0.1.jar \
-model bigdl_resnet_model_893 -reportingInterval 25
2019-02-15T23:36:03.979Z: Classifying images from 192.168.1.1:10000 with Resnet model bigdl_resnet_model_893, with 25 second intervals
2019-02-15T23:36:19.948Z: 25744 images received in interval - 3146 correct
2019-02-15T23:36:52.503Z: 222879 images received in interval - 22926 correct
2019-02-15T23:37:17.207Z: 224655 images received in interval - 25921 correct
2019-02-15T23:37:42.101Z: 225453 images received in interval - 23766 correct
2019-02-15T23:38:07.050Z: 225521 images received in interval - 23067 correct
2019-02-15T23:38:24.866Z: 75748 images received in interval - 8150 correct
2019-02-15T23:38:45.003Z: No input
2019-02-15T23:38:45.003Z: Stopping stream

2019-02-15T23:38:47.424Z: 1000000 images received in 128.6 seconds (6 intervals), or 7777 images/second. 106976 of 1000000 correctly inferred or 10.7%
```

## Where do trained models come from?

### keras_cifar10_trained_model_78.h5

Used program https://github.com/keras-team/keras/blob/master/examples/cifar10_cnn.py with one change:  
Added after line 116:  
  steps_per_epoch=len(x_train)/batch_size,  
Ran for 100 epochs, reached 77.7% accuracy on test set

### cifar10_ResNet20v1_model_91470.h5

Used program https://github.com/keras-team/keras/blob/master/examples/cifar10_resnet.py with two changes:  
Added after lines 369 and 425:  
  steps_per_epoch=len(x_train)/batch_size,  
Ran for 200 epochs, reached 91.47% accuracy on test set

### BDL_KERAS_CIFAR_CNN.bigdl.8/BDL_KERAS_CIFAR_CNN.bin.8

Created program based on https://github.com/intel-analytics/BigDL/blob/master/pyspark/bigdl/examples/keras/mnist_cnn.py  
Modified for CIFAR10 using convnet from https://github.com/keras-team/keras/blob/master/examples/cifar10_cnn.py, modified for Keras 1.2.2  
Ran to an accuracy target of 80%  
Saved trained model using trained_model.saveModel 
