#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=sz-v1.2504
docker build --no-cache -t $IMGNAME:$IMGVERSION .
