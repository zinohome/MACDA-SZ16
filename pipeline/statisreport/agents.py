#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import datetime

from app import app
from core.settings import settings
from pipeline.statisreport.models import input_topic
from utils.alertutil import Alertutil
from utils.log import log as log
from utils.tsutil import TSutil
import time


@app.timer(interval=settings.SEND_STATS_INTERVAL)
async def on_started():
    coachdict = {'1':'Tc1','2':'Mp1','3':'M1','4':'M2','5':'Mp2','6':'Tc2'}
    devicedict = {'EF_U1': '16001',
                  'CF_U1': '16002',
                  'Comp_U11': '16003',
                  'Comp_U12': '16004',
                  'FAD_U1': '16005',
                  'RAD_U1': '16006',
                  'EF_U2': '16007',
                  'CF_U2': '16008',
                  'Comp_U21': '16009',
                  'Comp_U22': '160010',
                  'FAD_U2': '160011',
                  'RAD_U2': '160012',
                  }
    log.debug("==========********** Get Statistic report data batch ==========**********")
    tu = TSutil()
    au = Alertutil()
    dev_mode = settings.DEV_MODE
    if dev_mode:
        statis_data = tu.get_statis_data('dev')
    else:
        statis_data = tu.get_statis_data('pro')
    # Generata statis data
    # Generata statis data
    log.debug('===============')
    log.debug(statis_data)
    statis_data_list = []
    if statis_data['len'] > 0:
        for item in statis_data['data']:
            dvc_no = item['msg_calc_dvc_no']
            dvc_no_list = [i for i in dvc_no.split('0') if i != '']
            if len(dvc_no_list) == 3:
                line_no = dvc_no_list[0]
                train_no = dvc_no_list[1]
                carbin_no = dvc_no_list[2]
                trainNo = f"{str(line_no).zfill(2)}{str(train_no).zfill(3)}"
                partCodepre = f"5{str(int(carbin_no))}"
                log.debug('line_no: %s, train_no: %s, carbin_no: %s' % (line_no, train_no, carbin_no))
                for code in au.partcodefield:
                    sdata = {}
                    line_name = str(line_no).replace(" ", "")
                    sdata['lineName'] = line_name
                    sdata['trainType'] = 'B'
                    sdata['trainNo'] = trainNo
                    sdata['partCode'] = str(au.getvalue('partcode', code.lower(), 'part_code')).replace('50', partCodepre)
                    sdata['serviceTime'] = int(datetime.now().timestamp() * 1000)
                    '''
                    if 'rad' in code or 'fad' in code or 'dmpexu' in code :
                        sdata['serviceTime'] = 0
                        sdata['serviceValue'] = item[f"dvc_{code}"]
                    else:
                        sdata['serviceTime'] = item[f"dvc_{code}"]
                        sdata['serviceValue'] = 0
                    '''
                    if 'rad' in code or 'fad' in code or 'dmpexu' in code :
                        sdata['serviceValue'] = int(item[f"dvc_{code}"])
                        sdata['mileage'] = 0
                    else:
                        sdata['serviceValue'] = 0
                        sdata['mileage'] = int(item[f"dvc_{code}"])
                    sdata['useTime'] = 0
                    sdata['flag'] = 0
                    statis_data_list.append(sdata)
    log.debug('statis_data_list is : %s' % statis_data_list)
    au.send_statistics(statis_data_list)
