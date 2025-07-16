#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import os
import shelve
import traceback
import uuid
from datetime import datetime
from decimal import Decimal

import requests

from core.settings import settings
import weakref
from utils.log import log as log
import simplejson as json
import numpy as np
import pandas as pd
import time

from utils.ptcode import PTCode
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
        self.cache_file = os.path.join(DATA_DIR, 'cache')
        self.send_cache_file = os.path.join(DATA_DIR, 'send_cache')
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

    '''
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

    '''

    def build_fault_list(self):
        tu = TSutil()
        pt_code = PTCode()
        try:
            dev_mode = settings.DEV_MODE
            if dev_mode:
                fault_records = tu.get_fault_statistic('dev_view_fault_timed_mat')
            else:
                fault_records = tu.get_fault_statistic('pro_view_fault_timed_mat')
        except Exception as e:
            log.error(f"获取故障统计数据失败: {e}")
            return []

        message_list = []
        # 打开shelve缓存
        with shelve.open(self.cache_file) as fault_cache:
            for record in fault_records:
                try:
                    # 检查是否需要过滤故障记录
                    fault_type = record.get('fault_type', '')
                    if fault_type.strip() == '故障' and not settings.SEND_FAULT_RECORD:
                        log.debug(f"过滤掉故障记录: {record.get('param_name', '')}")
                        continue
                    # 生成32位UUID作为faultId
                    fault_id = str(uuid.uuid4()).replace('-', '')
                    # 获取列车号和车厢号
                    dvc_train_no = record.get('dvc_train_no')
                    dvc_carriage_no = record.get('dvc_carriage_no')
                    if dvc_train_no is None or dvc_carriage_no is None:
                        log.warning(f"记录缺失列车号或车厢号: {record}")
                        continue
                    # 转换为字符串类型
                    train_no_str = str(dvc_train_no)
                    carriage_no_str = str(dvc_carriage_no)
                    # 获取列车编码，如果不存在则使用默认值
                    train_id = self.__traincode__.get(train_no_str, train_no_str)
                    # 获取车厢编码，如果不存在则使用默认值
                    coach_no = self.__carriagecode__.get(carriage_no_str, carriage_no_str)
                    # 获取故障发生时间，确保格式正确
                    begin_time = record.get('start_time', '')
                    if begin_time and not self._is_valid_datetime(begin_time):
                        log.warning(f"无效的时间格式: {begin_time}")
                        # 可以选择设置默认时间或跳过此记录
                    # 获取故障结束时间，确保格式正确
                    end_time = record.get('end_time', '')
                    #log.debug(end_time)
                    # 获取故障编码
                    #fault_code = record.get('param_name', '')
                    fault_code = pt_code.get_predict_code(record.get('param_name', ''),dvc_carriage_no)
                    # 处理begin_time，去除冒号和空格
                    clean_begin_time = begin_time.replace(':', '').replace(' ', '')
                    # 生成缓存键
                    cache_key = f"{train_no_str}-{carriage_no_str}-{fault_code.replace(' ', '')}-{clean_begin_time}"
                    # 生成缓存键
                    cache_key = f"{train_no_str}-{carriage_no_str}-{fault_code.replace(' ', '')}-{clean_begin_time}"
                    # 检查缓存中是否已有该fault_id
                    if cache_key in fault_cache:
                        fault_id = fault_cache[cache_key]
                        log.debug(f"从缓存中获取fault_id: {fault_id} 键: {cache_key}")
                        continue
                    else:
                        # 生成新的32位UUID作为faultId
                        fault_id = str(uuid.uuid4()).replace('-', '')
                        # 存入缓存
                        fault_cache[cache_key] = fault_id
                        log.debug(f"生成新的fault_id: {fault_id} 键: {cache_key}")
                    # 获取当前时间作为报文发送时间
                    create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # 构建单个报文
                    message = {
                        "faultId": fault_id,
                        "faultCode": fault_code,
                        "lineId": self.__linecode__,
                        "trainId": train_id,
                        "coachNo": coach_no,
                        "beginTime": begin_time,
                        "sysCode": self.__subsyscode__,
                        "createDate": create_date
                    }
                    message_list.append(message)
                except Exception as e:
                    log.error(f"处理故障记录时出错: {e}, 记录: {record}")
        return message_list


    def build_fault_update_list(self):
        tu = TSutil()
        pt_code = PTCode()
        try:
            dev_mode = settings.DEV_MODE
            if dev_mode:
                fault_records = tu.get_fault_statistic('dev_view_fault_timed_mat')
            else:
                fault_records = tu.get_fault_statistic('pro_view_fault_timed_mat')
        except Exception as e:
            log.error(f"获取故障统计数据失败: {e}")
            return []

        message_list = []
        # 打开shelve缓存
        with shelve.open(self.cache_file) as fault_cache:
            for record in fault_records:
                try:
                    # 检查是否需要过滤故障记录
                    fault_type = record.get('fault_type', '')
                    if fault_type.strip() == '故障' and not settings.SEND_FAULT_RECORD:
                        log.debug(f"过滤掉故障记录: {record.get('param_name', '')}")
                        continue
                    # 生成32位UUID作为faultId
                    fault_id = str(uuid.uuid4()).replace('-', '')
                    # 获取列车号和车厢号
                    dvc_train_no = record.get('dvc_train_no')
                    dvc_carriage_no = record.get('dvc_carriage_no')
                    if dvc_train_no is None or dvc_carriage_no is None:
                        log.warning(f"记录缺失列车号或车厢号: {record}")
                        continue
                    # 转换为字符串类型
                    train_no_str = str(dvc_train_no)
                    carriage_no_str = str(dvc_carriage_no)
                    # 获取列车编码，如果不存在则使用默认值
                    train_id = self.__traincode__.get(train_no_str, train_no_str)
                    # 获取车厢编码，如果不存在则使用默认值
                    coach_no = self.__carriagecode__.get(carriage_no_str, carriage_no_str)
                    # 获取故障发生时间，确保格式正确
                    begin_time = record.get('start_time', '')
                    if begin_time and not self._is_valid_datetime(begin_time):
                        log.warning(f"无效的时间格式: {begin_time}")
                        # 可以选择设置默认时间或跳过此记录
                    # 获取故障结束时间，确保格式正确
                    end_time = record.get('end_time', '')
                    if end_time and self._is_valid_datetime(end_time):
                        # 获取故障编码
                        #fault_code = record.get('param_name', '')
                        fault_code = pt_code.get_predict_code(record.get('param_name', ''),dvc_carriage_no)
                        # 处理begin_time，去除冒号和空格
                        clean_begin_time = begin_time.replace(':', '').replace(' ', '')
                        # 生成缓存键
                        cache_key = f"{train_no_str}-{carriage_no_str}-{fault_code.replace(' ', '')}-{clean_begin_time}"
                        # 生成缓存键
                        cache_key = f"{train_no_str}-{carriage_no_str}-{fault_code.replace(' ', '')}-{clean_begin_time}"
                        # 检查缓存中是否已有该fault_id
                        if cache_key in fault_cache:
                            fault_id = fault_cache[cache_key]
                            log.debug(f"从缓存中获取fault_id: {fault_id} 键: {cache_key}")
                        else:
                            # 生成新的32位UUID作为faultId
                            fault_id = str(uuid.uuid4()).replace('-', '')
                            # 存入缓存
                            fault_cache[cache_key] = fault_id
                            log.debug(f"生成新的fault_id: {fault_id} 键: {cache_key}")
                        # 获取当前时间作为报文发送时间
                        create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        # 构建单个报文
                        message = {
                            "faultId": fault_id,
                            "endTime": end_time,
                            "sysCode": self.__subsyscode__,
                            "createDate": create_date
                        }
                        message_list.append(message)
                except Exception as e:
                    log.error(f"处理故障记录时出错: {e}, 记录: {record}")
        return message_list

    def build_life_list(self):  # 方法名已更改
        tu = TSutil()
        pt_code = PTCode()
        try:
            dev_mode = settings.DEV_MODE
            if dev_mode:
                statistic_records = tu.get_fault_statistic('dev_view_health_equipment')
            else:
                statistic_records = tu.get_fault_statistic('pro_view_health_equipment')
        except Exception as e:
            log.error(f"获取故障统计数据失败: {e}")
            return []

        message_list = []

        for record in statistic_records:
            try:
                # 获取列车号和车厢号
                dvc_train_no = record.get('dvc_train_no')
                dvc_carriage_no = record.get('dvc_carriage_no')

                if dvc_train_no is None or dvc_carriage_no is None:
                    self.logger.warning(f"记录缺失列车号或车厢号: {record}")
                    continue
                # 转换为字符串类型
                train_no_str = str(dvc_train_no)
                carriage_no_str = str(dvc_carriage_no)

                # 获取车厢编码，如果不存在则使用默认值
                coach_no = self.__carriagecode__.get(carriage_no_str, carriage_no_str)

                # 获取参数名称
                param_name = record.get('param_name', '')
                # 获取部件编码
                componentCode = pt_code.get_part_code(param_name,dvc_carriage_no)
                componentName = pt_code.get_component_map(param_name)
                # 获取寿命比例，确保转换为字符串
                life_ratio = str(record.get('life_ratio', '0.00'))
                # 计算剩余时间 = baseline_data - param_value
                baseline_data = record.get('baseline_data', Decimal('0'))
                param_value = record.get('param_value', 0.0)

                # 处理Decimal类型和浮点数计算
                if isinstance(baseline_data, Decimal):
                    baseline_data = float(baseline_data)

                component_surplus_time = str(round(baseline_data - param_value, 0))
                component_rated_time = str(round(baseline_data, 0))

                # 获取健康状态
                health_status = record.get('health_status', '')

                # 获取当前时间作为报文发送时间
                create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 构建单个报文
                message = {
                    "lineId": "16号线",
                    "trainId": train_no_str,
                    "coachNo": coach_no,
                    "sysCode": self.__subsyscode__,
                    "componentCode": componentCode,
                    "componentName": componentName,
                    "predictionResult": life_ratio,
                    "componentSurplusTime": component_surplus_time,
                    "componentRatedTime": component_rated_time,
                    "componentSurplusKilo": "0",
                    "componentRatedKilo": "0",
                    "componentStatus": health_status,
                    "createDate": create_date
                }

                message_list.append(message)
            except Exception as e:
                self.logger.error(f"处理设备寿命记录时出错: {e}, 记录: {record}")

        return message_list
    def _is_valid_datetime(self, time_str):
        """验证字符串是否为有效的日期时间格式"""
        try:
            datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            return True
        except ValueError:
            return False

    def send_fault(self):
        """发送故障信息到指定URL"""
        # 获取故障列表
        message_list = self.build_fault_list()
        if not message_list:
            log.info("没有故障信息需要发送")
            return

        srvurl = settings.FAULT_RECORD_URL
        log.info(f"开始发送故障信息到: {srvurl}，共{len(message_list)}条")

        # 逐一发送消息
        for idx, message in enumerate(message_list, 1):
            try:
                # 发送POST请求
                response = requests.post(
                    srvurl,
                    json=message,
                    headers={"content-type": "application/json"},
                    timeout=10  # 设置超时时间为10秒
                )
                # 获取JSON响应
                response_data = response.json()
                # 检查业务状态
                http_status = response_data.get('httpStatus')
                if http_status == 'OK':
                    log.debug(f"第{idx}/{len(message_list)}条发送成功: {message.get('faultId')}")
                else:
                    error_msg = response_data.get('message', '未知错误')
                    log.debug(f"第{idx}/{len(message_list)}条发送失败: {error_msg}")

            except requests.exceptions.RequestException as e:
                log.error(f"第{idx}/{len(message_list)}条请求异常: {e}")
            except ValueError as e:
                log.error(f"第{idx}/{len(message_list)}条响应解析失败: {e}")
            except Exception as e:
                log.error(f"第{idx}/{len(message_list)}条发送时发生未知错误: {e}")

        log.info("故障信息发送完成")

    def send_fault_update(self):
        """发送故障信息到指定URL"""
        # 获取故障列表
        message_list = self.build_fault_update_list()
        if not message_list:
            log.info("没有故障信息需要发送")
            return

        srvurl = settings.FAULT_UPDATE_URL
        log.info(f"开始发送故障信息到: {srvurl}，共{len(message_list)}条")

        # 逐一发送消息
        for idx, message in enumerate(message_list, 1):
            try:
                # 发送POST请求
                response = requests.post(
                    srvurl,
                    json=message,
                    headers={"content-type": "application/json"},
                    timeout=10  # 设置超时时间为10秒
                )
                # 获取JSON响应
                response_data = response.json()
                # 检查业务状态
                http_status = response_data.get('httpStatus')
                if http_status == 'OK':
                    log.debug(f"第{idx}/{len(message_list)}条发送成功: {message.get('faultId')}")
                else:
                    error_msg = response_data.get('message', '未知错误')
                    log.debug(f"第{idx}/{len(message_list)}条发送失败: {error_msg}")

            except requests.exceptions.RequestException as e:
                log.error(f"第{idx}/{len(message_list)}条请求异常: {e}")
            except ValueError as e:
                log.error(f"第{idx}/{len(message_list)}条响应解析失败: {e}")
            except Exception as e:
                log.error(f"第{idx}/{len(message_list)}条发送时发生未知错误: {e}")

        log.info("故障信息发送完成")

    def send_life_report(self):
        """发送寿命信息到指定URL"""
        # 检查当前时间是否在允许的时间窗口内
        # 获取当前时间字符串（格式：HH:MM）
        current_time = datetime.now().strftime('%H:%M')

        # 定义时间窗口
        start_time = '14:00'
        end_time = '17:00'

        # 直接比较字符串大小
        if not (start_time <= current_time < end_time):
            log.info(f"当前时间 {current_time} 不在发送时间窗口 (16:30-17:00) 内，不发送")
            return

        # 获取今天的日期作为键（格式：YYYY-MM-DD）
        today = datetime.now().strftime('%Y-%m-%d')

        # 检查今天是否已经发送过
        try:
            with shelve.open(self.send_cache_file) as send_cache:
                if send_cache.get(today) == 1:
                    log.info(f"今天 ({today}) 已经发送过故障信息，不再重复发送")
                    return
        except Exception as e:
            log.error(f"检查发送记录失败: {e}")
            return  # 缓存操作失败时，为避免重复发送，直接返回
        # 获取寿命列表
        message_list = self.build_life_list()
        if not message_list:
            log.info("没有寿命信息需要发送")
            return

        srvurl = settings.LIFE_RECORD_URL
        log.info(f"开始发送寿命信息到: {srvurl}，共{len(message_list)}条")

        all_success = True  # 标记是否全部发送成功
        # 逐一发送消息
        for idx, message in enumerate(message_list, 1):
            try:
                # 发送POST请求
                response = requests.post(
                    srvurl,
                    json=message,
                    headers={"content-type": "application/json"},
                    timeout=10  # 设置超时时间为10秒
                )
                # 获取JSON响应
                response_data = response.json()
                # 检查业务状态
                http_status = response_data.get('httpStatus')
                if http_status == 'OK':
                    log.debug(f"第{idx}/{len(message_list)}条发送成功: {message.get('faultId')}")
                else:
                    error_msg = response_data.get('message', '未知错误')
                    log.debug(f"第{idx}/{len(message_list)}条发送失败: {error_msg}")
                    all_success = False

            except requests.exceptions.RequestException as e:
                log.error(f"第{idx}/{len(message_list)}条请求异常: {e}")
                all_success = False
            except ValueError as e:
                log.error(f"第{idx}/{len(message_list)}条响应解析失败: {e}")
                all_success = False
            except Exception as e:
                log.error(f"第{idx}/{len(message_list)}条发送时发生未知错误: {e}")
                all_success = False

        # 如果全部发送成功，记录今天已发送
        if all_success:
            try:
                with shelve.open(self.send_cache_file) as send_cache:
                    send_cache[today] = 1  # 标记今天已发送
                log.info(f"所有故障信息发送成功，已记录今日（{today}）发送状态")
            except Exception as e:
                log.error(f"记录发送状态失败: {e}")
        log.info("寿命信息发送完成")
if __name__ == '__main__':
    au = Alertutil()
    log.debug(str(au.getvalue('partcode', 'dwOPCount_FAD_U1'.lower(), 'part_code')))
    log.debug(au.__traincode__)
    log.debug(au.__carriagecode__)
    log.debug(au.__carriagecode__['6'])
    log.debug(au.__carriagecode__.get('6'))
    log.debug(au.__subsyscode__)
    log.debug(str(uuid.uuid4()).replace('-', ''))
    log.debug(au.build_fault_list())
    log.debug(au.build_fault_update_list())
    log.debug(au.build_life_list())
    #au.send_fault()
    #au.send_fault_update()
    #au.send_life_report()
