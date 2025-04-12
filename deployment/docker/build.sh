#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=nb-v1.2410
docker build --no-cache -t $IMGNAME:$IMGVERSION .
