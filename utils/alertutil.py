#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import os
import traceback
import requests

from core.settings import settings
import weakref
from utils.log import log as log
import simplejson as json
import numpy as np
import pandas as pd
import time

from utils.tsutil import TSutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'predictdata')

class Cached(type):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__cache = weakref.WeakValueDictionary()

    def __call__(self, *args):
        if args in self.__cache:
            return self.__cache[args]
        else:
            obj = super().__call__(*args)
            self.__cache[args] = obj
            return obj
class Alertutil(metaclass=Cached):
    def __init__(self):
        log.debug('========== Alertutil init : Code loading.')
        alertcodefile = os.path.join(DATA_DIR, 'alertcode.xlsx')
        partcodefile = os.path.join(DATA_DIR, 'partcode.xlsx')
        self.__alertcode__ = pd.read_excel(alertcodefile)
        self.__alertcode__['name'] = self.__alertcode__['name'].apply(str.lower)
        self.__partcode__ = pd.read_excel(partcodefile)
        self.__partcode__['name'] = self.__partcode__['name'].apply(str.lower)
        #log.debug(self.__partcode__['name'])
        self.__linecode__ = 'f8cbefdea3d511eb92d60433c2688e9e'
        traincodedata = """1633	43ffb124cf2847b4a68d8117fa666319
        1634	a72750bc6add4a2c85f4bc34fbe3e5b7
        1635	a9b69f6c0985443591b04a0b4ec295ec
        1636	38be4374bca74e6f9d386e618ed33f9a
        1637	dd0fd941eede4599b04c7f3448a59d03
        1638	728893e459364c52b84e4614e3bb07fc
        1639	488285820e374796bb4478314126bdd0
        1640	b0fa5537603949aea15dc1e846e9f7fd
        1641	849a7bd624524356b7037685d1131244
        1642	4082e7f9378d41978a912e73919c5069
        1643	8cb64dabe73b403f85cd875613da0e38
        1644	5bdda87df1d74dd09df676bbf14a52f9"""

        # 将数据按行分割，再按制表符分割每行，转换为字典
        self.__traincode__  = {line.split('\t')[0].strip(): line.split('\t')[1].strip() for line in traincodedata.split('\n')}
        carriagecodedata = """1	A1
        2	B1
        3	C1
        4	C2
        5	B2
        6	A2"""
        # 将数据按行分割，再按制表符分割每行，转换为字典
        self.__carriagecode__ = {line.split('\t')[0].strip(): line.split('\t')[1].strip() for line in
                              carriagecodedata.split('\n')}
        self.__subsyscode__ = '07'
        log.debug('========== Alertutil init : Code loaded.')

    def getvalue(self, codetype, rowvalue, colname):
        rvalue = ''
        df = self.__alertcode__
        if codetype == 'partcode':
            df = self.__partcode__
        row = df.loc[df['name'] == rowvalue]
        if not row.empty:
            if colname in row.columns.values:
                rvalue = row[colname].values[0]
        return rvalue

    @property
    def predictfield(self):
        return ['ref_leak_u11', 'ref_leak_u12', 'ref_leak_u21', 'ref_leak_u22', 'f_cp_u1', 'f_cp_u2', 'f_fas', 'f_ras', 'cabin_overtemp', 'f_presdiff_u1', 'f_presdiff_u2', 'f_ef_u11', 'f_ef_u12', 'f_ef_u21', 'f_ef_u22', 'f_cf_u11', 'f_cf_u12', 'f_cf_u21', 'f_cf_u22', 'f_exufan', 'f_fas_u11', 'f_fas_u12', 'f_fas_u21', 'f_fas_u22', 'f_aq_u1', 'f_aq_u2']

    @property
    def alertfield(self):
        col = self.__alertcode__['name']
        return [c for c in col.tolist() if c not in self.predictfield]

    @property
    def partcodefield(self):
        col = self.__partcode__['name']
        return col.tolist()

    def send_statistics(self, statslist):
        srvurl = settings.STATS_RECORD_URL
        #log.debug(srvurl)
        #headers = {"content-type":"application/json","x-hasura-admin-secret":"passw0rd"}
        headers = {"content-type":"application/json"}
        data = json.dumps(statslist, encoding="utf-8", ensure_ascii=False)
        #log.debug(data)
        if settings.SEND_STATS_RECORD:
            try:
                response = requests.post(srvurl, data.encode(), headers=headers)
                log.debug('Send Statistic data with response code: [%s] ' % response.status_code)
            except Exception as exp:
                log.error('Exception at alrtutil.send_statistics() %s ' % exp)
                traceback.print_exc()
    def send_lifereport(self, statslist):
        srvurl = settings.LIFE_RECORD_URL
        #log.debug(srvurl)
        #headers = {"content-type":"application/json","x-hasura-admin-secret":"passw0rd"}
        headers = {"content-type":"application/json"}
        data = json.dumps(statslist, encoding="utf-8", ensure_ascii=False)
        #log.debug(data)
        if settings.SEND_LIFE_RECORD:
            try:
                response = requests.post(srvurl, data.encode(), headers=headers)
                log.debug('Send LifeReport data with response code: [%s] ' % response.status_code)
            except Exception as exp:
                log.error('Exception at alrtutil.send_lifereport() %s ' % exp)
                traceback.print_exc()

    def send_status(self, statuslist):
        srvurl = settings.SYS_STATUS_URL
        #log.debug(srvurl)
        #headers = {"content-type":"application/json","x-hasura-admin-secret":"passw0rd"}
        headers = {"content-type":"application/json"}
        data = json.dumps(statuslist, encoding="utf-8", ensure_ascii=False)
        #log.debug(data)
        if settings.SEND_STATUS_RECORD:
            try:
                response = requests.post(srvurl, data.encode(), headers=headers)
                log.debug('Send Status data with response code: [%s] ' % response.status_code)
            except Exception as exp:
                log.error('Exception at alrtutil.send_status() %s ' % exp)
                traceback.print_exc()

    def send_predict(self, predictlist):
        srvurl = settings.FAULT_RECORD_URL
        #log.debug(srvurl)
        #headers = {"content-type":"application/json","x-hasura-admin-secret":"passw0rd"}
        headers = {"content-type":"application/json"}
        data = json.dumps(predictlist, encoding="utf-8", ensure_ascii=False)
        #log.debug(data)
        if settings.SEND_FAULT_RECORD:
            try:
                response = requests.post(srvurl, data.encode(), headers=headers)
                log.debug('Send Alert data with response code: [%s] ' % response.status_code)
            except Exception as exp:
                log.error('Exception at alrtutil.send_predict() %s ' % exp)
                traceback.print_exc()



if __name__ == '__main__':
    au = Alertutil()
    log.debug(str(au.getvalue('partcode', 'dwOPCount_FAD_U1'.lower(), 'part_code')))
    log.debug(au.__traincode__)
    log.debug(au.__carriagecode__)
