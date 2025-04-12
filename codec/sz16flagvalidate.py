#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time

from codec.sz16flag import Sz16flag
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
from utils.log import log as log
import simplejson as json

if __name__ == '__main__':
    rdict = Sz16flag.from_file_to_dict('sz16')
    for k,v in rdict.items():
        log.debug('%s = [%s]' % (k,v))
    log.debug(len(rdict.items()))
    log.debug(rdict)
    log.debug(json.dumps(rdict))
    keylst = list(rdict.keys()).copy()
    log.debug(keylst)
    log.debug(str(int(time.mktime(time.strptime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "%Y-%m-%d %H:%M:%S")))))
    log.debug(int(time.time()))  # 秒级时间戳
    log.debug(str(int(round(time.time() * 1000))))  # 毫秒级时间戳




