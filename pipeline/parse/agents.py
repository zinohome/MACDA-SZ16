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
from codec.nb67 import Nb67
from core.settings import settings
from pipeline.fetcher.models import output_schema
from pipeline.parse.models import input_topic, output_topic, json_schema
from utils.log import log as log

@app.agent(input_topic)
async def parse_signal(stream):
    dev_mode = settings.DEV_MODE
    src_mode = settings.SRC_MODE
    async for data in stream:
        log.debug("-------------------- Get binary data --------------------")
        jschema = {
            "type": "struct",
            "name": "ACSignal"
        }
        if dev_mode:
            # Parse data and send to parsed topic
            parsed_dict = {}
            if src_mode.strip().lower() == 'bin':
                parsed_dict = Nb67.from_bytes_to_dict(data)
            else:
                datadict = json.loads(str(data, encoding='utf-8'))
                if 'message_data' in datadict.keys():
                    parsed_dict = Nb67.from_bytes_to_dict(binascii.a2b_hex(datadict['message_data']))
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
            archivetopic = app.topic(archivetopicname, partitions=settings.TOPIC_PARTITIONS, value_serializer='json')
            await archivetopic.send(key=key, value=out_record)
        else:
            # Parse data and send to parsed topic
            parsed_dict = {}
            if src_mode.strip().lower() == 'bin':
                parsed_dict = Nb67.from_bytes_to_dict(data)
            else:
                datadict = json.loads(str(data, encoding='utf-8'))
                if 'message_data' in datadict.keys():
                    parsed_dict = Nb67.from_bytes_to_dict(binascii.a2b_hex(datadict['message_data']))
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
            archivetopic = app.topic(archivetopicname, partitions=settings.TOPIC_PARTITIONS, value_serializer='json')
            await archivetopic.send(key=key, value=out_record)
