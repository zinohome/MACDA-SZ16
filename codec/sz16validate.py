#!/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Time    : 2025/04/12 17:42
# @Author  : ZhangJun
# @FileName: sz16validate.py

from codec.sz16 import Sz16
from utils.log import log as log
import simplejson as json
import time

if __name__ == '__main__':
    rdict = Sz16.from_file_to_dict('sz16')
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