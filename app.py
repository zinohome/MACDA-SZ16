#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA

import faust
from core.settings import settings

app = faust.App(
    f"{settings.APP_TITLE}-{settings.RUN_MODE}",
    version = float(settings.APP_VERSION),
    broker = settings.KAFKA_BOOTSTRAP_SERVER,
    store = settings.STORE_URI,
    topic_partitions = settings.TOPIC_PARTITIONS,
    topic_allow_declare=settings.TOPIC_ALLOW_DECLARE,
    topic_disable_leader=settings.TOPIC_DISABLE_LEADER
)
app.web.blueprints.add('/stats/', 'faust.web.apps.stats:blueprint')
run_mode = settings.RUN_MODE
#app.discover('pipeline.parse')
#app.discover('pipeline.batchparse')
#app.discover('pipeline.store')
#app.discover('pipeline.batchstore')
#app.discover('pipeline.status')
#app.discover('pipeline.lifereport')
#app.discover('pipeline.statisreport')
#app.discover('pipeline.faultreport')
#app.discover('pipeline.predict')


if run_mode.strip().lower() == 'parse':
    # run parse
    app.discover('pipeline.batchparse')
elif run_mode.strip().lower() == 'store':
    # run store
    app.discover('pipeline.batchstore')
elif run_mode.strip().lower() == 'refresh':
    # run refresh
    app.discover('pipeline.refresh_mview')
elif run_mode.strip().lower() == 'predict':
    # run refresh
    app.discover('pipeline.predict')
else:
    # run predict
    #app.discover('pipeline.status')
    #app.discover('pipeline.lifereport')
    #app.discover('pipeline.predict')
    app.discover('pipeline.faultreport')
    #app.discover('pipeline.predict', 'pipeline.faultreport')


    #app.discover('pipeline.predict', 'pipeline.faultreport','pipeline.statisreport')
#app.discover('pipeline.batchparse','pipeline.batchstore','pipeline.predict','pipeline.faultreport','pipeline.statisreport')
#app.discover('pipeline.batchstore','pipeline.predict')
