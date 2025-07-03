#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=sz-v1.2506.2
docker build --no-cache -t $IMGNAME:$IMGVERSION .
