#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=sz-v1.2506.1
docker build --no-cache -t $IMGNAME:$IMGVERSION .
