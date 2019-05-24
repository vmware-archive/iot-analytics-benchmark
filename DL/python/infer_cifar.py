
"""
infer_cifar.py: read pre-labeled CIFAR10 images from standard input, infer object using saved pre-trained keras model, compare to label

In one window:
  $ python3 send_images_cifar.py | nc <dest IP address> <dest port>

In another window:
  $ nc -lk <port> | python3 infer_cifar.py <arguments>

optional arguments:
  Arguments:
  -h         | --help                       print help message
  -m <value> | --modelPath <value>          model path                 Required
  -r <value> | --reportingInterval <value>  reporting interval (sec)   Default: 10

Uses CIFAR10 dataset from https://www.cs.toronto.edu/~kriz/cifar.html
(Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009, https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf)

Copyright (c) 2019 VMware, Inc.

This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

import keras
from keras.models import load_model
from time import time, gmtime, strftime
import numpy as np
import sys
import argparse

parser = argparse.ArgumentParser(description='Infer CIFAR10 images using pre-trained Keras model')
parser.add_argument("-m", "--modelPath", type=str, dest="modelPath", required=True)
parser.add_argument("-r", "--reportingInterval", type=int, dest="reportingInterval", default=10)
args = parser.parse_args()
model_path=args.modelPath
reporting_interval=args.reportingInterval

model = load_model(model_path)
print('Loaded trained model %s ' % model_path)
print("Start send program")

n_images = 0
n_correct = 0
# Read labeled string from standard in, start clock when receive first string
for labeled_image_str in sys.stdin:
  if n_images == 0:
    start_time = time()
  if len(labeled_image_str) == 1:  # Empty string indicates end of images
    break
  else:
    # Pull first character as label, turn remainder of string into float np.array in correct shape for model
    label = int(labeled_image_str[0])
    image = np.array([float(x) for x in labeled_image_str[1:].split()]).reshape((1,32,32,3))
    # Use model to classify image, compare to label, count correct percentage
    prediction = model.predict(image).argmax()
    n_images += 1
    if label == prediction:
      n_correct += 1
    if n_images%reporting_interval == 0:
      print('%sZ: %d images classified' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), n_images))
    
finish_time = time()
print('Inferenced %d images in %.1f seconds or %.1f images/second, ' % (n_images, finish_time-start_time, 1.0*n_images/(finish_time-start_time)), end='')
print('with %d or %.1f%% correctly classified' % (n_correct, (100.0*n_correct)/n_images))

sc.stop()
