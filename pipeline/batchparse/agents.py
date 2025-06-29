#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import binascii
import simplejson as json
from app import app
from codec.sz16 import Sz16
from codec.sz16flag import Sz16flag
from core.settings import settings
from pipeline.fetcher.models import output_schema
from pipeline.batchparse.models import input_topic, output_topic, json_schema
from utils.log import log as log


@app.agent(input_topic)
async def parse_signal(stream):
    dev_mode = settings.DEV_MODE
    src_mode = settings.SRC_MODE
    async for datas in stream.take(settings.TSDB_PARSE_BATCH, within=settings.TSDB_PARSE_BATCH_TIME):
        log.debug("-------------------- Batch Get binary data --------------------")
        for data in datas:
            carriage_data_list = extract(data)
            if carriage_data_list:
                for cdata in carriage_data_list:
                    jschema = {
                        "type": "struct",
                        "name": "ACSignal"
                    }
                    if dev_mode:
                        # Parse cdata and send to parsed topic
                        parsed_dict = {}
                        flag_dict = {}
                        if src_mode.strip().lower() == 'bin':
                            flag_dict = Sz16flag.from_bytes_to_dict(cdata)
                        else:
                            datadict = json.loads(str(cdata, encoding='utf-8'))
                            if 'message_data' in datadict.keys():
                                flag_dict = Sz16flag.from_bytes_to_dict(binascii.a2b_hex(datadict['message_data']))
                            else:
                                log.debug("-------------------- No message flag data --------------------")
                        if flag_dict['dvc_flag'] == 1:
                            if src_mode.strip().lower() == 'bin':
                                parsed_dict = Sz16.from_bytes_to_dict(cdata)
                            else:
                                datadict = json.loads(str(cdata, encoding='utf-8'))
                                if 'message_data' in datadict.keys():
                                    parsed_dict = Sz16.from_bytes_to_dict(binascii.a2b_hex(datadict['message_data']))
                                else:
                                    log.debug("-------------------- No message data --------------------")
                            out_record = {"schema": jschema, "payload": parsed_dict}
                            if dev_mode:
                                key = f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_parse_time']}"
                            else:
                                key = f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}"
                            log.debug(
                                "---------- Parsed data with key : %s" % f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}")
                            await output_topic.send(key=key, value=out_record, schema=output_schema)
                            # Send json to Archive topics
                            archivetopicname = f"MACDA-archive-{settings.PARSED_TOPIC_NAME}-{parsed_dict['msg_calc_dvc_no']}"
                            archivetopic = app.topic(archivetopicname, partitions=settings.TOPIC_PARTITIONS,
                                                     value_serializer='json')
                            await archivetopic.send(key=key, value=out_record)
                    else:
                        # Parse cdata and send to parsed topic
                        parsed_dict = {}
                        flag_dict = {}
                        if src_mode.strip().lower() == 'bin':
                            flag_dict = Sz16flag.from_bytes_to_dict(cdata)
                        else:
                            datadict = str(cdata, encoding='utf-8')
                            flag_dict = Sz16flag.from_bytes_to_dict(binascii.a2b_hex(datadict))
                            log.debug("-------------------- Binary flage_dict data --------------------")
                            log.debug(flag_dict)
                        if flag_dict['dvc_flag'] == 1:
                            if src_mode.strip().lower() == 'bin':
                                parsed_dict = Sz16.from_bytes_to_dict(cdata)
                            else:
                                datadict = str(cdata, encoding='utf-8')
                                parsed_dict = Sz16.from_bytes_to_dict(binascii.a2b_hex(datadict))
                            out_record = {"schema": jschema, "payload": parsed_dict}
                            if dev_mode:
                                key = f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_parse_time']}"
                            else:
                                key = f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}"
                            log.debug(
                                "---------- Parsed data with key : %s" % f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}")
                            await output_topic.send(key=key, value=out_record, schema=output_schema)
                            # Send json to Archive topics
                            archivetopicname = f"MACDA-archive-{settings.PARSED_TOPIC_NAME}-{parsed_dict['msg_calc_dvc_no']}"
                            archivetopic = app.topic(archivetopicname, partitions=settings.TOPIC_PARTITIONS,
                                                     value_serializer='json')
                            await archivetopic.send(key=key, value=out_record)


def extract(data: bytes) -> list[bytes]:
    """
    从Kafka消息中提取各车厢空调数据帧

    参数:
        data: 二进制字节流数据

    返回:
        列表，包含各车厢的400字节数据帧（不足6节时用None填充）
    """
    # 验证输入是否为二进制类型
    if not isinstance(data, bytes):
        raise TypeError(f"输入类型必须为bytes，实际为{type(data).__name__}")

    # 检查数据长度是否符合要求
    # if len(data) > 5122:
    #     raise ValueError(f"数据长度过大，标准为5122，实际{len(data)}字节")

    if data[86:90] == b'0642' and data[246:248] == b'A5':
        log.info("*****************深16二期数据********************")
        # 从字节偏移248-249读取COMID数量（1字节）
        comid_count = int(data[248 - 249], 16)

        # 验证COMID数量是否合理
        if comid_count < 1 or comid_count > 6:
            raise ValueError(f"无效的COMID数量: {comid_count}，应在1-6之间")

        # 初始化结果列表（6节车厢，不足的用None填充）
        carriages_data = [None] * 6

        # 按车厢解析数据
        for i in range(comid_count):
            # 计算车厢数据报文头和数据帧的起始地址
            header_start = 266 + i * 808  # 每节车厢间隔404字节（4字节报文头 + 400字节数据帧）
            frame_start = header_start + 8  # 数据帧紧跟在报文头后

            # 提取车厢数据帧（400字节）
            frame_data = data[frame_start: frame_start + 800]

            # 验证数据帧长度
            if len(frame_data) != 800:
                raise ValueError(f"车厢{i + 1}的数据帧长度不足，期望800，实际{len(frame_data)}字节")

            # 存储到结果列表中（索引0-5对应车厢1-6）
            carriages_data[i] = frame_data

        return carriages_data
