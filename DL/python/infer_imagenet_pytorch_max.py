"""
infer_imagenet_pytorch_max.py: GPU based imagenet inference maximum speed test:
  reads pre-labeled ImageNet images into memory, infers image classification using pretrained models of various types
  calculates accuracy, runs for specified time with printouts every specified iterations, prints out images per second
Usage:
  Run in a container based on nvcr.io/nvidia/pytorch:20.02-py3

  $ python infer_imagenet_pytorch_max.py <arguments>  DIR

positional arguments:
  DIR                   Path to dataset

optional arguments:
  -h, --help            Show this help message and exit
  -a ARCH, --arch ARCH  Model architecture: alexnet | densenet121 |
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
                        wide_resnet50_2 (default: resnet50)
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        Batch size (default: 256). Must be less than the number of ImageNet val images in DIR
  -d DURATION, --duration DURATION
                        Run duration in seconds (default: 600)
  -i INTERVAL, --interval INTERVAL
                        Iterations per print (default: 10)

Uses code from https://github.com/pytorch/examples/blob/master/imagenet/main.py and models from pytorch.org

Copyright (c) 2029 VMware, Inc.
This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License.
This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
"""

import argparse
import os
from time import time, gmtime, strftime

import torch
import torch.nn as nn
import torch.backends.cudnn as cudnn
import torch.utils.data
import torchvision.transforms as transforms
import torchvision.datasets as datasets
import torchvision.models as models

def accuracy(output, target, topk=(1,)):
  """Computes the accuracy over the k top predictions for the specified values of k"""
  with torch.no_grad():
    maxk = max(topk)
    batch_size = target.size(0)

    _, pred = output.topk(maxk, 1, True, True)
    pred = pred.t()
    correct = pred.eq(target.view(1, -1).expand_as(pred))

    res = []
    for k in topk:
      correct_k = correct[:k].view(-1).float().sum(0, keepdim=True)
      res.append(correct_k.mul_(100.0 / batch_size))
    return res

# Read model names
model_names = sorted(name for name in models.__dict__
  if name.islower() and not name.startswith("__")
  and callable(models.__dict__[name]))

parser = argparse.ArgumentParser(description='PyTorch ImageNet Inference Maximum Speed Test')
parser.add_argument('data', metavar='DIR',
                    help='Path to dataset containing bucketed ImageNet validation images')
parser.add_argument('-a', '--arch', metavar='ARCH', default='resnet50',
                    choices=model_names,
                    help='Model architecture: ' +
                        ' | '.join(model_names) +
                        ' (default: resnet50)')
parser.add_argument('-b', '--batch-size', default=256, type=int,
                    help='Batch size (default: 256). Must be less than the number of ImageNet val images in DIR')

parser.add_argument('-d', '--duration', default=600, type=int,
                    help='Run duration in seconds (default: 600)')

parser.add_argument('-i', '--interval', default=10, type=int,
                    help='Iterations per print (default: 10)')

args = parser.parse_args()

valdir = os.path.join(args.data, 'val')
model_arch = args.arch
batch_size = args.batch_size
duration = args.duration
interval = args.interval

print('Running image classification using pre-trained model %s with data from %s, for %d seconds, reporting every %d iterations, with batch_size %d' 
  %(model_arch, valdir, duration, interval, batch_size))

# Read model from NVIDIA site if not cached
model = models.__dict__[model_arch](pretrained=True)
print('Model loaded')

cudnn.benchmark = True

if model_arch.startswith('alexnet') or model_arch.startswith('vgg'):
   model.features = torch.nn.DataParallel(model.features)
   model.cuda()
else:
   model = torch.nn.DataParallel(model).cuda()
   
criterion = nn.CrossEntropyLoss().cuda(None)

normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                   std=[0.229, 0.224, 0.225])

val_loader = torch.utils.data.DataLoader(
  datasets.ImageFolder(valdir, transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    normalize,
  ])),
  batch_size, shuffle=False,
  num_workers=0, pin_memory=True)

model.eval()

n_classified = 0

with torch.no_grad():
  # Grab batch_size number of images from disk, load into memory, iterate with these. NB: number of images on disk must be greater than batch_size
  (images, target) = list(enumerate(val_loader))[0][1]
  if (len(images) < batch_size):
    print('%d images loaded from %s is less than batch_size (%d). Lower batch_size or add more images to directory. Exiting' %(len(images), valdir, batch_size))
    exit()
  else:
    print('%d images loaded' %(len(images)))

  images = images.cuda(None, non_blocking=True)
  target = target.cuda(None, non_blocking=True)

  print('%s.%03dZ: Test started' % (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000))
  start_time = time()
  iteration = 1

  while True:
    output = model(images)
    n_classified += len(images)
    acc1, acc5 = accuracy(output, target, topk=(1, 5))
    if (iteration % interval == 0):
        print('%s.%03dZ: Iteration %d: %d images inferred. Acc@1= %.3f Acc@5= %.3f' %
             (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, iteration, n_classified, acc1, acc5))
    iteration += 1
    elapsed_time = time() - start_time
    if elapsed_time > duration:
      break

print('%s.%03dZ: Test completed: %d images inferred in %.1f sec or %.1f images/second' %
  (strftime("%Y-%m-%dT%H:%M:%S", gmtime()), (time()*1000)%1000, n_classified, elapsed_time, n_classified/elapsed_time))
