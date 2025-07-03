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


@app.timer(interval=settings.SEND_FAULT_INTERVAL)
async def on_started():
    coachdict = {'1':'Tc1','2':'Mp1','3':'M1','4':'M2','5':'Mp2','6':'Tc2',}
    log.debug("==========********** Get Fault report data batch ==========**********")
    tu = TSutil()
    au = Alertutil()
    au.send_fault()
    au.send_fault_update()
    au.send_life_report()