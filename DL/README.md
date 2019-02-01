

# iot-analytics-benchmark

## Introduction

IoT Analytics Benchmark DL consists of neural network-based Deep Learning image classification programs run on a stream of images.

The programs run Keras and BigDL image classifiers using pre-trained models and the CIFAR10 image set. For each type of classifier there is a program that sends the images
as a series of encoded strings and a second program that reads those string, converts them back to images, and infers which of the 10 CIFAR10 classes that image belongs to.

The Keras classifiers are Python-based single node programs for running on an IoT edge gateway.

The BigDL classifiers are Spark-based distributed programs.

Uses Intel's BigDL library (see https://github.com/intel-analytics/BigDL-Tutorials) and 

CIFAR10 dataset from https://www.cs.toronto.edu/~kriz/cifar.html

(Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009, https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf)

## Installation

- Install Spark (Spark 2.4.0 Standalone tested here)
   - Spark single node installation: obtain latest version from <http://spark.apache.org/downloads.html> and unzip

- Install python3 on all nodes, add numpy, keras and tensorflow with pip

- Install nc on driver node (`yum install nc`)

- For purposes of this documentation, a symbolic link to the Spark code on the driver system is assumed. For example:
    `ln -s /root/spark-2.4.0-bin-hadoop2.7 /root/spark`

- Add spark/bin directory to `$PATH`

- Set log level from INFO to ERROR (suggested for cleaner output, especially of iotstream). In `spark/conf`:
   `cp log4j.properties.template log4j.properties`
   Set `log4j.rootCategory=ERROR, console`

- Clone or download and unzip project

## Project Files

File                         | Use
:----                        | :---
`infer_cifar.py`             | Python program to classify CIFAR10 images using CNN or RESNET model
`send_images_cifar.py`       | Send images to infer_cifar.py
`keras_cifar10_trained_model_78.h5`   | Trained CNN model - 78% accurate
`icifar10_ResNet20v1_model_91470.h5`  | Trained RESNET model - 91.47% accurate
`README.md`                  | This file


## Program usage (run any program with -h flag to see parameters)

### Python-based CNN/RESNET CIFAR10 image classifier

In one shell:

`python3 send_images_cifar.py [-h] [-s] [-i IMAGESPERSEC] [-t TOTALIMAGES] | nc <dest IP address>  <dest port>`

where:

Parameter      | Use
:---------     | :---
IMAGESPERSEC   | Images per second to send - defaults to 10
TOTALIMAGES    | Total number of images to send - defaults to 100

Specify -s to subtract image mean from each image value - use for RESNET model

In another shell:

`nc -lk <port> | python3 infer_cifar.py [-h] -m MODELPATH [-r REPORTINGINTERVAL]`

where:

Parameter          | Use
:---------         | :---
MODELPATH          | location of trained model file - required
REPORTINGINTERVAL  | Reporting interval - defaults to every 100 images sent

Example

```
$ nc -lk 10000 | python3 infer_cifar.py --modelPath cifar10_ResNet20v1_model_91470.h5 --reportingInterval 1000
Using TensorFlow backend.
Loaded trained model cifar10_ResNet20v1_model_91470.h5
Start send program  <run send_images_cifar.py - next command>
2019-01-31T02:44:45Z: 1000 images classifed
...
2019-01-31T02:45:38Z: 10000 images classifed
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

## Releases & Major Branches

Master

## Contributing

The iot-analytics-benchmark project team welcomes contributions from the community. If you wish to contribute code and you have not
signed our contributor license agreement (CLA), our bot will update the issue when you open a Pull Request. For any
questions about the CLA process, please refer to our [FAQ](https://cla.vmware.com/faq). For more detailed information,
refer to [CONTRIBUTING.md](CONTRIBUTING.md). Any questions or suggestions, please contact the author, Dave Jaffe at djaffe@vmware.com.

## License

Copyright (c) 2019 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
