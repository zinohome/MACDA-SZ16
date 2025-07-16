#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=sz-v1.2506.3
docker build --no-cache -t $IMGNAME:$IMGVERSION .
