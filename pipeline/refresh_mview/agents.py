#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA

from app import app
from core.settings import settings
from pipeline.faultreport.models import input_topic
from utils.alertutil import Alertutil
from utils.log import log as log
from utils.tsutil import TSutil
import time


@app.timer(interval=settings.SEND_STATS_INTERVAL)
async def on_started():
    log.debug("==========********** REFRESH MATERIALIZED VIEW CONCURRENTLY ==========**********")
    tu = TSutil()
    tu.refresh_all_materialized_views()