"""
send_images_cifar_stream.py: sends CIFAR10 images encoded as a string to a Spark Streaming inference program

Usage: python3 send_images_cifar_stream.py [-h] [-i IMAGESPERSEC] [-t TOTALIMAGES] | nc -lk <port>
optional arguments:
  -h, --help            show this help message and exit
  -i IMAGESPERSEC, --imagesPerSec IMAGESPERSEC
  -t TOTALIMAGES, --totalImages TOTALIMAGES

Uses CIFAR10 dataset from https://www.cs.toronto.edu/~kriz/cifar.html
(Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009, https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf)

Copyright (c) 2019 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

import argparse
import random
from time import time, gmtime, strftime, sleep, monotonic
import sys
from io import StringIO
import numpy as np
from math import exp
from keras.datasets import cifar10

parser = argparse.ArgumentParser(description='Send CIFAR10 images encoded as strings')
parser.add_argument("-i", "--imagesPerSec", type=int, dest="imagesPerSec", default=10)
parser.add_argument("-t", "--totalImages", type=int, dest="totalImages", default=100)
args = parser.parse_args()
images_per_second=args.imagesPerSec; total_images=args.totalImages

def accurate_wait(wait_in_seconds):
    waitUntil = monotonic() +  wait_in_seconds
    while (waitUntil > monotonic()):
      pass

print("%sZ: Loading and normalizing the CIFAR10 data" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime())), file=sys.stderr)
(_, _), (test_images, test_labels) = cifar10.load_data()
n_images = test_images.shape[0]
n_labels = test_labels.shape[0]

# First, write labeled images to a list
labeled_images = []
for i in range(n_images):
  string = StringIO()
  np.savetxt(string, test_images[i].ravel().reshape(1,3072), fmt='%d') # 3072 = 32x32x3
  # Insert (single character) label in front of string, cut final '\n' from string
  labeled_images.append(str(test_labels.item(i)) + string.getvalue()[:-1])

print("%sZ: Pausing 15 seconds - start infer_cifar_stream.py" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime())), file=sys.stderr)
sleep(15)
print("%sZ: Sending %d images per second for a total of %d images" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), images_per_second, total_images), file=sys.stderr)

# Send images from array, wrapping if necessary
for i in range(total_images):
  print(labeled_images[i%n_images])
  sys.stdout.flush()
  # Use lognormal distribution to generate a positive random wait time with mean determined from images_per_second and long tail
  mean_wait = float(1.0/images_per_second)
  # Set standard deviation to half the mean_wait
  std_dev = mean_wait/2.0
  fudge_factor = .7 # Needed to reduce wait time to compensate for computation/network time - set empirically
  accurate_wait(fudge_factor*mean_wait*random.lognormvariate(mean_wait,std_dev)/exp(mean_wait + std_dev**2/2))
  if (((i+1) % images_per_second == 0) or (i == total_images-1)):
    print("%sZ: %d images sent" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), i+1), file=sys.stderr)

print("%sZ: Image stream ended - keeping socket open for 120 seconds" % (strftime("%Y-%m-%dT%H:%M:%S", gmtime())), file=sys.stderr)
# Add 120 seconds at end to keep socket open while Spark streaming completes
sleep(120)
