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
from pipeline.predict.models import input_topic
from utils.log import log as log
from utils.sensorpolyfit import SensorPolyfit
from utils.tsutil import TSutil
from collections import Counter


@app.agent(input_topic)
async def store_signal(stream):
    tu = TSutil()
    predictcounter = Counter()
    async for data in stream:
        pdvcno = data['payload']['msg_calc_dvc_no']
        if predictcounter[pdvcno] == 0:
            predictcounter[pdvcno] = 1
        else:
            predictcounter[pdvcno] += 1
        if predictcounter[pdvcno] > settings.PREDICT_SKIP_BATCH:
            log.debug("==========---------- Predict for [ %s ] ==========----------" % pdvcno)
            #log.debug("==========---------- Get predict data ==========----------")
            dev_mode = settings.DEV_MODE
            mode = 'dev'
            if not dev_mode:
                mode = 'pro'
            #predictdata = tu.get_predictdata(mode, pdvcno)
            predictdata = tu.predict(mode, pdvcno)
            if len(predictdata) > 0:
                if dev_mode:
                    key = f"{data['payload']['msg_calc_dvc_no']}-{data['payload']['msg_calc_parse_time']}"
                else:
                    key = f"{data['payload']['msg_calc_dvc_no']}-{data['payload']['msg_calc_dvc_time']}"
                log.debug("Add predict data  with key : %s" % key)
                predictdata['msg_calc_dvc_time'] = data['payload']['msg_calc_dvc_time']
                predictdata['msg_calc_parse_time'] = data['payload']['msg_calc_parse_time']
                predictdata['msg_calc_dvc_no'] = data['payload']['msg_calc_dvc_no']
                predictdata['msg_calc_train_no'] = data['payload']['msg_calc_train_no']
                if dev_mode:
                    tu.insert_predictdata('dev_predict', predictdata)
                else:
                    tu.insert_predictdata('pro_predict', predictdata)
            predictcounter[pdvcno] = 0
        else:
            pass
            #log.debug("==========---------- Skip for [ %s ] ==========----------" % pdvcno)

