

# iot-analytics-benchmark DL

## Introduction

IoT Analytics Benchmark DL consists of neural network-based Deep Learning image classification programs run on a stream of images.

The programs run Keras and BigDL image classifiers using pre-trained models and the CIFAR10 image set. For each type of classifier there is a program that sends the images
as a series of encoded strings and a second program that reads those string, converts them back to images, and infers which of the 10 CIFAR10 classes that image belongs to.

The Keras classifier is a Python-based single node program for running on an IoT edge gateway.

The BigDL classifiers (Python and Scala versions) are Spark-based distributed programs, some using Spark Streaming to read and infer the input encoded images, other
reading pre-loaded images directly from memory.

The infer_imagenet_pytorch_max.py test is a GPU-based PyTorch inference engine that loads ImageNet images into memory and classifies them as fast as possible using various models

Uses Intel's BigDL library (see <https://github.com/intel-analytics/BigDL-Tutorials>) and  
CIFAR10 dataset from <https://www.cs.toronto.edu/~kriz/cifar.html>   
See [Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009](https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf)

## Installation

- Install python3 and pip3 on all nodes, add numpy, keras and tensorflow with pip3  

  Example on Centos 7:
  ```
  yum install https://centos7.iuscommunity.org/ius-release.rpm
  yum install python36u python36u-pip
  ln -s /usr/bin/python3.6 /usr/bin/python3
  ln -s /usr/bin/pip3.6 /usr/bin/pip3
  pip3 install --upgrade pip
  pip3 install numpy keras tensorflow
  ```

- Install nc on all nodes (`yum install nc`)

- For the Spark and BigDL programs:

- Install Spark 
  - Spark single node installation: obtain latest version from <http://spark.apache.org/downloads.html> and unzip
  - Spark release 2.4.3, using package "Prebuilt for Apache Hadoop 2.7 and later", tested here

- Install BigDL (Version 0.8.0 for Spark 2.4.1 tested here)

  Example on Centos 7:
  ```
  wget https://repo1.maven.org/maven2/com/intel/analytics/bigdl/dist-spark-2.4.0-scala-2.11.8-all/0.8.0/dist-spark-2.4.0-scala-2.11.8-all-0.8.0-dist.zip
  mkdir BigDL; mv dist-spark-2.4.0-scala-2.11.8-all-0.8.0-dist.zip BigDL; cd BigDL
  unzip dist-spark-2.4.0-scala-2.11.8-all-0.8.0-dist.zip
  pip3 install BigDL==0.8.0
  ```

  - You will use `lib/bigdl-SPARK_2.4-0.8.0-jar-with-dependencies.jar` in commands below

- For purposes of this documentation, a symbolic link to the Spark code on the driver system is assumed. For example:
  `ln -s /root/spark-2.4.3-bin-hadoop2.7 /root/spark`

- Add spark/bin directory to `$PATH`. For example:  

  edit `~/.bash_profile`  
  add following lines: 
  ```
  export PATH=$PATH:/root/spark/bin
  export SPARK_HOME=/root/spark
  export PYSPARK_PYTHON=python3
  ```
  `source .bash_profile`


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
`infer_cifar_max.py`                  | Maximum throughput Spark BigDL Python program to classify CIFAR10 images using CNN model
`infer_cifar_stream.py`               | Spark Streaming BigDL program to classify CIFAR10 images using CNN model
`send_images_cifar_stream.py`         | Send images to infer_cifar_stream.py
`BDL_KERAS_CIFAR_CNN.bigdl.8`         | Trained CNN model definition file for BigDL program - 80% accurate
`BDL_KERAS_CIFAR_CNN.bin.8`           | Trained CNN model weights file for BigDL program - 80% accurate
`infer_imagenet_max.scala`            | Maximum throughput Spark BigDL Scala program to classify ImageNet images using ResNet50 model
`infer_cifar_stream.scala`            | Spark Streaming BigDL scala program to classify CIFAR10 images using ResNet model
`send_images_cifar_stream.scala`      | Send images to infer_cifar_stream.scala
`bigdl_resnet_model_887`              | Trained ResNet model for Scala BigDL program - 88.7% accurate
`build.sbt`                           | SBT build file
`assembly.sbt`                        | SBT assembly file
`infer_imagenet_pytorch_max.py`       | Maximum throughput PyTorch program to classify ImageNet images using various models
`README.md`                           | This file


## Program usage (run any program with -h flag to see parameters)

### Python Keras CNN/ResNet CIFAR10 image classifier

In one shell, `cd <path>/iot-analytics-benchmark-master/DL/python`, then:

`nc -lk <port> | python3 infer_cifar.py [-h] -m MODELPATH [-r REPORTINGINTERVAL]`

Parameter          | Use
:---------         | :---
MODELPATH          | Location of trained model file - required
REPORTINGINTERVAL  | Reporting interval - defaults to every 100 images sent

Wait for program to output "Start send program" then, in a second shell on the same or different server, in the same directory:

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


### Maximum throughput Spark BigDL CNN CIFAR10 image classifier

```
cd <path>/iot-analytics-benchmark-master/DL/python
spark-submit <Spark config params> --jars <path>/bigdl-SPARK_2.4-0.8.0-jar-with-dependencies.jar infer_cifar_max.py <arguments>
```

Arguments:
```
  -h          | --help                      print help message
  -md <value> | --modelDefsPath <value>     model definitions path     Required
  -mw <value> | --modelWeightsPath <value>  model weights path         Required
  -d  <value> | --duration <value>          duration (sec)             Default: 10
```
Example

```
$ spark-submit --master spark://<host>:7077 --driver-memory 40G --conf spark.executor.instances=12 \
--conf spark.cores.max=84 --conf spark.executor.cores=7 --executor-memory 104g \
--jars <path>/BigDL/lib/bigdl-SPARK_2.4-0.8.0-jar-with-dependencies.jar infer_cifar_max.py \
--modelDefsPath BDL_KERAS_CIFAR_CNN.bigdl.8 --modelWeightsPath BDL_KERAS_CIFAR_CNN.bin.8 --duration 60
...
Loading trained model from BDL_KERAS_CIFAR_CNN.bigdl.8 (definition) and BDL_KERAS_CIFAR_CNN.bin.8 (weights)
2019-07-09T17:28:10.825Z: Running inference loop for 60 seconds
2019-07-09T17:28:17.422Z: Iteration 1: 50000 images inferred. Correct predictions: 49872  Pct correct: 0.9974
2019-07-09T17:29:12.130Z: Iteration 26: 50000 images inferred. Correct predictions: 49872  Pct correct: 0.9974
2019-07-09T17:29:12.130Z: Test completion: 1300000 images inferred in 61.3 sec or 21205.4 images/second
```


### Spark Streaming BigDL CNN CIFAR10 image classifier

In one shell, `cd <path>/iot-analytics-benchmark-master/DL/python`, then:

`python3 send_images_cifar_stream.py [-h] [-i IMAGESPERSEC] [-t TOTALIMAGES] | nc -lk <port>`

Parameter      | Use
:---------     | :---
IMAGESPERSEC   | Images per second to send - defaults to 10
TOTALIMAGES    | Total number of images to send - defaults to 100

Specify -s to subtract image mean from each image value - use for ResNet model

Wait for "Pausing 15 seconds - start infer_cifar_stream.py", then in a second shell on the same or different server, in the same directory:

```
spark-submit <Spark config params> --jars <path>/bigdl-SPARK_2.4-0.8.0-jar-with-dependencies.jar infer_cifar_stream.py \  
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
--executor-memory 104g --jars <path>/BigDL/lib/bigdl-SPARK_2.4-0.8.0-jar-with-dependencies.jar infer_cifar_stream.py \
--modelDefsPath BDL_KERAS_CIFAR_CNN.bigdl.8 --modelWeightsPath BDL_KERAS_CIFAR_CNN.bin.8 -r 25
...
2019-01-31T15:54:40.199Z: Loaded trained model definitions BDL_KERAS_CIFAR_CNN.bigdl.8 and weights BDL_KERAS_CIFAR_CNN.bin.8
2019-01-31T15:54:40.199Z: Starting reading streaming data from 192.168.1.1:10000 at interval 25 seconds
2019-01-31T15:55:14.059Z: Interval 1:  images received=126280   images correctly predicted=101124
...
2019-01-31T15:56:43.345Z: Interval 5:  images received=782   images correctly predicted=613
2019-01-31T15:57:05.017Z: Stopping stream

2019-01-31T15:57:07.422Z: 1000000 images received in 116.0 seconds (5 intervals), or 8619 images/second  Correct predictions: 800700  Pct correct: 80.1
```

### Maximum throughput Spark BigDL Scala program to classify ImageNet images using ResNet50 model

Compile Scala code into assembly with dependencies included:

- Install Scala (2.11.8 tested) and SBT (1.1.0 tested)

  ```
  cd <path>/iot-analytics-benchmark-master/DL/scala
  <Modify build.sbt for correct Spark and Scala versions if necessary>
  sbt assembly
  ```
- Creates `iotstreamdl-assembly-0.0.1.jar`


To run:

Download ImageNet ilsvrc2012 validation set from http://image-net.org. The folder containing the folders (n01440764, etc.) with the raw JPEG images
is what the --folder argument points to.  
          
Download ResNet50 trained model from Facebook: https://github.com/facebook/fb.resnet.torch/tree/master/pretrained#trained-resnet-torch-models  

Run program:
```
$ spark-submit <Spark config params> --class com.intel.analytics.bigdl.examples.imageclassification.infer_imagenet_max <path>/iotstreamdl-assembly-0.0.1.jar <arguments>
```

Arguments:
```
  -f, --folder    <value>  location of test image data  Required
  -m, --modelPath <value>  location of model            Required
  -d, --duration  <value>  duration (sec)               Default: 60
  -b, --batchSize <value>  batch size                   Default: number of executors
```

Example

```
$ spark-submit --master spark://192.168.1.1:7077 --driver-memory 100g --conf spark.cores.max=256 \
--conf spark.executor.cores=16 --conf spark.executor.instances=16 --executor-memory 100g \
--class com.intel.analytics.bigdl.example.imageclassification.infer_imagenet_max \
<path>/iotstreamdl-assembly-0.0.1.jar --modelPath /root/resnet-50.t7 --folder /root/ilsvrc2012/val2K
2019-08-20T23:24:10.960Z: Loading trained model from /root/resnet-50.t7
2019-08-20T23:24:12.769Z: Loading images from /root/ilsvrc2012/val2K
2019-08-20T23:25:07.892Z: 2000 images found, using each image 5x for 10000 total images
2019-08-20T23:25:08.034Z: Parallelization complete
2019-08-20T23:25:10.158Z: Running inference loop for 600 seconds
2019-08-20T23:25:41.136Z: Iteration 1: 10000 images inferred. 8467 or 84.7% predicted correctly
...
2019-08-20T23:35:16.651Z: Iteration 26: 10000 images inferred. 8452 or 84.5% predicted correctly
2019-08-20T23:35:16.651Z: Test completion: 260000 images inferred in 606.5 sec or 428.7 images/second
```

### Spark Streaming BigDL ResNet CIFAR10 Scala image classifier

Compile Scala code into assembly with dependencies included:

- Install Scala (2.11.8 tested) and SBT (1.1.0 tested)

  ```
  cd <path>/iot-analytics-benchmark-master/DL/scala
  <Modify build.sbt for correct Spark and Scala versions if necessary>
  sbt assembly
  ```
- Creates `iotstreamdl-assembly-0.0.1.jar`


To run, in one shell:

Download CIFAR10 dataset:
```
wget https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz
tar xzvf cifar-10-binary.tar.gz  # Creates directory cifar-10-batches-bin
```
Run program:
```
java -Xmx128g -cp <path>/iotstreamdl-assembly-0.0.1.jar com.intel.analytics.bigdl.models.resnet.send_images_cifar_stream \
  <arguments> | nc -lk <port>
```

Arguments:
```
  -f, --folder <value>        the location of Cifar10 dataset  Default: cifar-10-batches-bin
  -i, --imagesPerSec <value>  images per second                Default: 10
  -t, --totalImages <value>   total images                     Default: 100
```

Wait for "Pausing 15 seconds - start infer_cifar_stream", then in a second shell on the same or different server:

```
spark-submit <Spark config params> --class com.intel.analytics.bigdl.models.resnet.infer_cifar_stream \
  <path>/iotstreamdl-assembly-0.0.1.jar <arguments>
```

Arguments:
```
  -r, --reportingInterval <value> reporting interval (sec)   Default: 1
  -i, --sourceIPAddress <value>   source IP address          Default: 192.168.1.1
  -p, --sourcePort <value>        source port                Default: 10000
  -m, --model <value>             model                      Required
  -b, --batchSize <value>         batch size                 Default: 2000
```

Example

```
$ java -Xmx100g -cp <path>/iotstreamdl-assembly-0.0.1.jar com.intel.analytics.bigdl.models.resnet.send_images_cifar_stream \
  --imagesPerSec 9000 --totalImages 360000 | nc -lk 11000
Will send 9000 images per second for a total of 360000 images
Pausing 15 seconds - start image_stream_cifar
2019-05-21T18:33:07.853Z: Sending images
2019-05-21T18:33:15.380Z: 9000 images sent
...
2019-05-21T18:34:08.920Z: 360000 images sent
2019-05-21T18:34:08.920Z: Sent 360000 images in 61.1 seconds

$ spark-submit --master spark://<host>:7077 --driver-memory 100G --conf spark.cores.max=96 --conf spark.executor.cores=8 \
  --executor-memory 104g --class com.intel.analytics.bigdl.models.resnet.infer_cifar_stream \
  <path>/iotstreamdl-assembly-0.0.1.jar --model bigdl_resnet_model_887 --reportingInterval 10 --sourcePort 11000 --batchSize 2400
2019-05-21T18:33:06.567Z: Classifying images from 192.168.1.1:11000 with Resnet model bigdl_resnet_model_887, with 10 second intervals
2019-05-21T18:33:28.124Z: 40100 images received in interval - 35574 or 88.7% predicted correctly
...
2019-05-21T18:34:17.806Z: 59539 images received in interval - 52806 or 88.7% predicted correctly
2019-05-21T18:34:20.003Z: No input
2019-05-21T18:34:20.003Z: Stopping stream

2019-05-21T18:34:22.431Z: 360000 images received in 56.4 seconds (6 intervals), or 6382 images/second. 319320 of 360000 or 88.7% predicted correctly
```

### Maximum throughput PyTorch program to classify ImageNet images using various models

Needs to be run in container based on nvcr.io/nvidia/pytorch:20.02-py3

Run program:
```

$ python infer_imagenet_pytorch_max.py [-h] [-a ARCH] [-b BATCH_SIZE] [-d DURATION] [-i INTERVAL] DIR

positional arguments:
  DIR                   path to dataset containing bucketed ImageNet validation images

optional arguments:
  -h, --help            show this help message and exit
  -a ARCH, --arch ARCH  model architecture: alexnet | densenet121 |
                        densenet161 | densenet169 | densenet201 | googlenet |
                        inception_v3 | mnasnet0_5 | mnasnet0_75 | mnasnet1_0 |
                        mnasnet1_3 | mobilenet_v2 | resnet101 | resnet152 |
                        resnet18 | resnet34 | resnet50 | resnext101 |
                        resnext101_32x8d | resnext152 | resnext50 |
                        resnext50_32x4d | shufflenet_v2_x0_5 |
                        shufflenet_v2_x1_0 | shufflenet_v2_x1_5 |
                        shufflenet_v2_x2_0 | squeezenet1_0 | squeezenet1_1 |
                        vgg11 | vgg11_bn | vgg13 | vgg13_bn | vgg16 | vgg16_bn
                        | vgg19 | vgg19_bn | wide_resnet101_2 |
                        wide_resnet50_2 (default: resnet18)
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        batch size (default: 256). Must be less than the number of ImageNet val images in DIR
  -d DURATION, --duration DURATION
                        Run duration in seconds (default: 600)
  -i INTERVAL, --interval INTERVAL
                        Iterations per print (default: 10)
```

Example

```
python infer_imagenet_pytorch_max.py -a resnet50 -d 60 -i 10 -b 1024 /data
Running image classification using pre-trained model resnet50 with data from /data/val, for 60 seconds, reporting every 10 iterations, with batch_size 1024
Model loaded
1024 images loaded
2020-04-14T02:38:09.969Z: Test started
2020-04-14T02:38:38.570Z: Iteration 10: 10240 images inferred. Acc@1= 89.648 Acc@5= 97.363
2020-04-14T02:39:05.646Z: Iteration 20: 20480 images inferred. Acc@1= 89.648 Acc@5= 97.363
2020-04-14T02:39:16.115Z: Test completed: 20480 images inferred in 66.1 sec or 309.6 images/second

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

### bigdl_resnet_model_887

Ran BigDL ResNet50 model (https://github.com/intel-analytics/BigDL/blob/master/spark/dl/src/main/scala/com/intel/analytics/bigdl/models/resnet/TrainCIFAR10.scala)  
Used model saved by checkpoint after 100 epochs.
