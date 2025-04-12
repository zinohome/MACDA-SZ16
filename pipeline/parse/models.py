#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import faust
from app import app
from core.settings import settings

input_topic = app.topic(settings.SOURCE_TOPIC_NAME, value_serializer='raw')

class ACSignal(faust.Record):
    msg_header_code01: int
    msg_header_code02: int
    msg_length: int
    msg_src_dvc_no: int
    msg_host_dvc_no: int
    msg_type: int
    msg_frame_no: int
    msg_line_no: int
    msg_train_type: int
    msg_train_no: int
    msg_carriage_no: int
    msg_protocal_version: int
    dvc_flag: int
    dvc_train_no: int
    dvc_carriage_no: int
    dvc_year: int
    dvc_month: int
    dvc_day: int
    dvc_hour: int
    dvc_minute: int
    dvc_second: int
    cfbk_ef_u11: int
    cfbk_cf_u11: int
    cfbk_comp_u11: int
    cfbk_comp_u12: int
    cfbk_ap_u11: int
    cfbk_ef_u21: int
    cfbk_cf_u21: int
    cfbk_comp_u21: int
    cfbk_comp_u22: int
    cfbk_ap_u21: int
    cfbk_tpp_u1: int
    cfbk_tpp_u2: int
    cfbk_ev_u1: int
    cfbk_ev_u2: int
    cfbk_ewd: int
    bocflt_ef_u11: int
    bocflt_ef_u12: int
    bocflt_cf_u11: int
    bocflt_cf_u12: int
    bflt_vfd_u11: int
    bflt_vfd_com_u11: int
    bflt_vfd_u12: int
    bflt_vfd_com_u12: int
    blpflt_comp_u11: int
    bscflt_comp_u11: int
    bscflt_vent_u11: int
    blpflt_comp_u12: int
    bscflt_comp_u12: int
    bscflt_vent_u12: int
    bflt_fad_u11: int
    bflt_fad_u12: int
    bflt_rad_u11: int
    bflt_rad_u12: int
    bflt_ap_u11: int
    bflt_expboard_u1: int
    bflt_frstemp_u1: int
    bflt_rnttemp_u1: int
    bflt_splytemp_u11: int
    bflt_splytemp_u12: int
    bflt_coiltemp_u11: int
    bflt_coiltemp_u12: int
    bflt_insptemp_u11: int
    bflt_insptemp_u12: int
    bflt_lowpres_u11: int
    bflt_lowpres_u12: int
    bflt_highpres_u11: int
    bflt_highpres_u12: int
    bflt_diffpres_u1: int
    bocflt_ef_u21: int
    bocflt_ef_u22: int
    bocflt_cf_u21: int
    bocflt_cf_u22: int
    bflt_vfd_u21: int
    bflt_vfd_com_u21: int
    bflt_vfd_u22: int
    bflt_vfd_com_u22: int
    blpflt_comp_u21: int
    bscflt_comp_u21: int
    bscflt_vent_u21: int
    blpflt_comp_u22: int
    bscflt_comp_u22: int
    bscflt_vent_u22: int
    bflt_fad_u21: int
    bflt_fad_u22: int
    bflt_rad_u21: int
    bflt_rad_u22: int
    bflt_ap_u21: int
    bflt_expboard_u2: int
    bflt_frstemp_u2: int
    bflt_rnttemp_u2: int
    bflt_splytemp_u21: int
    bflt_splytemp_u22: int
    bflt_coiltemp_u21: int
    bflt_coiltemp_u22: int
    bflt_insptemp_u21: int
    bflt_insptemp_u22: int
    bflt_lowpres_u21: int
    bflt_lowpres_u22: int
    bflt_highpres_u21: int
    bflt_highpres_u22: int
    bflt_diffpres_u2: int
    bflt_emergivt: int
    bflt_vehtemp_u1: int
    bflt_vehtemp_u2: int
    bflt_airmon_u1: int
    bflt_airmon_u2: int
    bflt_currentmon: int
    bflt_tcms: int
    bflt_tempover: int
    bflt_powersupply_u1: int
    bflt_powersupply_u2: int
    bflt_exhaustfan: int
    bflt_exhaustval: int
    fas_sys: int
    ras_sys: int
    tic: int
    load: int
    tveh_1: int
    tveh_2: int
    aq_t_u1: int
    aq_h_u1: int
    aq_co2_u1: int
    aq_tvoc_u1: int
    aq_formald_u1: int
    aq_pm2_5_u1: int
    aq_pm10_u1: int
    wmode_u1: int
    presdiff_u1: int
    fas_u1: int
    ras_u1: int
    fadpos_u1: int
    radpos_u1: int
    f_cp_u11: int
    i_cp_u11: int
    v_cp_u11: int
    p_cp_u11: int
    suckt_u11: int
    suckp_u11: int
    sp_u11: int
    eevpos_u11: int
    highpress_u11: int
    sas_u11: int
    ices_u11: int
    f_cp_u12: int
    i_cp_u12: int
    v_cp_u12: int
    p_cp_u12: int
    suckt_u12: int
    suckp_u12: int
    sp_u12: int
    eevpos_u12: int
    highpress_u12: int
    sas_u12: int
    ices_u12: int
    aq_t_u2: int
    aq_h_u2: int
    aq_co2_u2: int
    aq_tvoc_u2: int
    aq_formald_u2: int
    aq_pm2_5_u2: int
    aq_pm10_u2: int
    wmode_u2: int
    presdiff_u2: int
    fas_u2: int
    ras_u2: int
    fadpos_u2: int
    radpos_u2: int
    f_cp_u21: int
    i_cp_u21: int
    v_cp_u21: int
    p_cp_u21: int
    suckt_u21: int
    suckp_u21: int
    sp_u21: int
    eevpos_u21: int
    highpress_u21: int
    sas_u21: int
    ices_u21: int
    f_cp_u22: int
    i_cp_u22: int
    v_cp_u22: int
    p_cp_u22: int
    suckt_u22: int
    suckp_u22: int
    sp_u22: int
    eevpos_u22: int
    highpress_u22: int
    sas_u22: int
    ices_u22: int
    i_ef_u11: int
    i_ef_u12: int
    i_cf_u11: int
    i_cf_u12: int
    i_ef_u21: int
    i_ef_u22: int
    i_cf_u21: int
    i_cf_u22: int
    i_hvac_u1: int
    i_hvac_u2: int
    i_exufan: int
    dwpower: int
    dwemerg_op_tm: int
    dwemerg_op_cnt: int
    dwef_op_tm_u11: int
    dwcf_op_tm_u11: int
    dwcp_op_tm_u11: int
    dwcp_op_tm_u12: int
    dwfad_op_cnt_u1: int
    dwrad_op_cnt_u1: int
    dwef_op_cnt_u11: int
    dwcf_op_cnt_u11: int
    dwcp_op_cnt_u11: int
    dwcp_op_cnt_u12: int
    dwef_op_tm_u21: int
    dwcf_op_tm_u21: int
    dwcp_op_tm_u21: int
    dwcp_op_tm_u22: int
    dwfad_op_cnt_u2: int
    dwrad_op_cnt_u2: int
    dwef_op_cnt_u21: int
    dwcf_op_cnt_u21: int
    dwcp_op_cnt_u21: int
    dwcp_op_cnt_u22: int
    dwexufan_op_tm: int
    dwexufan_op_cnt: int
    dwdmpexu_op_cnt: int
    msg_calc_dvc_no: int
    msg_calc_train_no: int
    msg_calc_dvc_time: str
    msg_calc_parse_time: str

json_schema = {
        "type": "struct",
        "name": "ACSignal",
        "fields": [
            {"name": "msg_header_code01", "type": "int"},
            {"name": "msg_header_code02", "type": "int"},
            {"name": "msg_length", "type": "int"},
            {"name": "msg_src_dvc_no", "type": "int"},
            {"name": "msg_host_dvc_no", "type": "int"},
            {"name": "msg_type", "type": "int"},
            {"name": "msg_frame_no", "type": "int"},
            {"name": "msg_line_no", "type": "int"},
            {"name": "msg_train_type", "type": "int"},
            {"name": "msg_train_no", "type": "int"},
            {"name": "msg_carriage_no", "type": "int"},
            {"name": "msg_protocal_version", "type": "int"},
            {"name": "dvc_flag", "type": "int"},
            {"name": "dvc_train_no", "type": "int"},
            {"name": "dvc_carriage_no", "type": "int"},
            {"name": "dvc_year", "type": "int"},
            {"name": "dvc_month", "type": "int"},
            {"name": "dvc_day", "type": "int"},
            {"name": "dvc_hour", "type": "int"},
            {"name": "dvc_minute", "type": "int"},
            {"name": "dvc_second", "type": "int"},
            {"name": "cfbk_ef_u11", "type": "int"},
            {"name": "cfbk_cf_u11", "type": "int"},
            {"name": "cfbk_comp_u11", "type": "int"},
            {"name": "cfbk_comp_u12", "type": "int"},
            {"name": "cfbk_ap_u11", "type": "int"},
            {"name": "cfbk_ef_u21", "type": "int"},
            {"name": "cfbk_cf_u21", "type": "int"},
            {"name": "cfbk_comp_u21", "type": "int"},
            {"name": "cfbk_comp_u22", "type": "int"},
            {"name": "cfbk_ap_u21", "type": "int"},
            {"name": "cfbk_tpp_u1", "type": "int"},
            {"name": "cfbk_tpp_u2", "type": "int"},
            {"name": "cfbk_ev_u1", "type": "int"},
            {"name": "cfbk_ev_u2", "type": "int"},
            {"name": "cfbk_ewd", "type": "int"},
            {"name": "bocflt_ef_u11", "type": "int"},
            {"name": "bocflt_ef_u12", "type": "int"},
            {"name": "bocflt_cf_u11", "type": "int"},
            {"name": "bocflt_cf_u12", "type": "int"},
            {"name": "bflt_vfd_u11", "type": "int"},
            {"name": "bflt_vfd_com_u11", "type": "int"},
            {"name": "bflt_vfd_u12", "type": "int"},
            {"name": "bflt_vfd_com_u12", "type": "int"},
            {"name": "blpflt_comp_u11", "type": "int"},
            {"name": "bscflt_comp_u11", "type": "int"},
            {"name": "bscflt_vent_u11", "type": "int"},
            {"name": "blpflt_comp_u12", "type": "int"},
            {"name": "bscflt_comp_u12", "type": "int"},
            {"name": "bscflt_vent_u12", "type": "int"},
            {"name": "bflt_fad_u11", "type": "int"},
            {"name": "bflt_fad_u12", "type": "int"},
            {"name": "bflt_rad_u11", "type": "int"},
            {"name": "bflt_rad_u12", "type": "int"},
            {"name": "bflt_ap_u11", "type": "int"},
            {"name": "bflt_expboard_u1", "type": "int"},
            {"name": "bflt_frstemp_u1", "type": "int"},
            {"name": "bflt_rnttemp_u1", "type": "int"},
            {"name": "bflt_splytemp_u11", "type": "int"},
            {"name": "bflt_splytemp_u12", "type": "int"},
            {"name": "bflt_coiltemp_u11", "type": "int"},
            {"name": "bflt_coiltemp_u12", "type": "int"},
            {"name": "bflt_insptemp_u11", "type": "int"},
            {"name": "bflt_insptemp_u12", "type": "int"},
            {"name": "bflt_lowpres_u11", "type": "int"},
            {"name": "bflt_lowpres_u12", "type": "int"},
            {"name": "bflt_highpres_u11", "type": "int"},
            {"name": "bflt_highpres_u12", "type": "int"},
            {"name": "bflt_diffpres_u1", "type": "int"},
            {"name": "bocflt_ef_u21", "type": "int"},
            {"name": "bocflt_ef_u22", "type": "int"},
            {"name": "bocflt_cf_u21", "type": "int"},
            {"name": "bocflt_cf_u22", "type": "int"},
            {"name": "bflt_vfd_u21", "type": "int"},
            {"name": "bflt_vfd_com_u21", "type": "int"},
            {"name": "bflt_vfd_u22", "type": "int"},
            {"name": "bflt_vfd_com_u22", "type": "int"},
            {"name": "blpflt_comp_u21", "type": "int"},
            {"name": "bscflt_comp_u21", "type": "int"},
            {"name": "bscflt_vent_u21", "type": "int"},
            {"name": "blpflt_comp_u22", "type": "int"},
            {"name": "bscflt_comp_u22", "type": "int"},
            {"name": "bscflt_vent_u22", "type": "int"},
            {"name": "bflt_fad_u21", "type": "int"},
            {"name": "bflt_fad_u22", "type": "int"},
            {"name": "bflt_rad_u21", "type": "int"},
            {"name": "bflt_rad_u22", "type": "int"},
            {"name": "bflt_ap_u21", "type": "int"},
            {"name": "bflt_expboard_u2", "type": "int"},
            {"name": "bflt_frstemp_u2", "type": "int"},
            {"name": "bflt_rnttemp_u2", "type": "int"},
            {"name": "bflt_splytemp_u21", "type": "int"},
            {"name": "bflt_splytemp_u22", "type": "int"},
            {"name": "bflt_coiltemp_u21", "type": "int"},
            {"name": "bflt_coiltemp_u22", "type": "int"},
            {"name": "bflt_insptemp_u21", "type": "int"},
            {"name": "bflt_insptemp_u22", "type": "int"},
            {"name": "bflt_lowpres_u21", "type": "int"},
            {"name": "bflt_lowpres_u22", "type": "int"},
            {"name": "bflt_highpres_u21", "type": "int"},
            {"name": "bflt_highpres_u22", "type": "int"},
            {"name": "bflt_diffpres_u2", "type": "int"},
            {"name": "bflt_emergivt", "type": "int"},
            {"name": "bflt_vehtemp_u1", "type": "int"},
            {"name": "bflt_vehtemp_u2", "type": "int"},
            {"name": "bflt_airmon_u1", "type": "int"},
            {"name": "bflt_airmon_u2", "type": "int"},
            {"name": "bflt_currentmon", "type": "int"},
            {"name": "bflt_tcms", "type": "int"},
            {"name": "bflt_tempover", "type": "int"},
            {"name": "bflt_powersupply_u1", "type": "int"},
            {"name": "bflt_powersupply_u2", "type": "int"},
            {"name": "bflt_exhaustfan", "type": "int"},
            {"name": "bflt_exhaustval", "type": "int"},
            {"name": "fas_sys", "type": "int"},
            {"name": "ras_sys", "type": "int"},
            {"name": "tic", "type": "int"},
            {"name": "load", "type": "int"},
            {"name": "tveh_1", "type": "int"},
            {"name": "tveh_2", "type": "int"},
            {"name": "aq_t_u1", "type": "int"},
            {"name": "aq_h_u1", "type": "int"},
            {"name": "aq_co2_u1", "type": "int"},
            {"name": "aq_tvoc_u1", "type": "int"},
            {"name": "aq_formald_u1", "type": "int"},
            {"name": "aq_pm2_5_u1", "type": "int"},
            {"name": "aq_pm10_u1", "type": "int"},
            {"name": "wmode_u1", "type": "int"},
            {"name": "presdiff_u1", "type": "int"},
            {"name": "fas_u1", "type": "int"},
            {"name": "ras_u1", "type": "int"},
            {"name": "fadpos_u1", "type": "int"},
            {"name": "radpos_u1", "type": "int"},
            {"name": "f_cp_u11", "type": "int"},
            {"name": "i_cp_u11", "type": "int"},
            {"name": "v_cp_u11", "type": "int"},
            {"name": "p_cp_u11", "type": "int"},
            {"name": "suckt_u11", "type": "int"},
            {"name": "suckp_u11", "type": "int"},
            {"name": "sp_u11", "type": "int"},
            {"name": "eevpos_u11", "type": "int"},
            {"name": "highpress_u11", "type": "int"},
            {"name": "sas_u11", "type": "int"},
            {"name": "ices_u11", "type": "int"},
            {"name": "f_cp_u12", "type": "int"},
            {"name": "i_cp_u12", "type": "int"},
            {"name": "v_cp_u12", "type": "int"},
            {"name": "p_cp_u12", "type": "int"},
            {"name": "suckt_u12", "type": "int"},
            {"name": "suckp_u12", "type": "int"},
            {"name": "sp_u12", "type": "int"},
            {"name": "eevpos_u12", "type": "int"},
            {"name": "highpress_u12", "type": "int"},
            {"name": "sas_u12", "type": "int"},
            {"name": "ices_u12", "type": "int"},
            {"name": "aq_t_u2", "type": "int"},
            {"name": "aq_h_u2", "type": "int"},
            {"name": "aq_co2_u2", "type": "int"},
            {"name": "aq_tvoc_u2", "type": "int"},
            {"name": "aq_formald_u2", "type": "int"},
            {"name": "aq_pm2_5_u2", "type": "int"},
            {"name": "aq_pm10_u2", "type": "int"},
            {"name": "wmode_u2", "type": "int"},
            {"name": "presdiff_u2", "type": "int"},
            {"name": "fas_u2", "type": "int"},
            {"name": "ras_u2", "type": "int"},
            {"name": "fadpos_u2", "type": "int"},
            {"name": "radpos_u2", "type": "int"},
            {"name": "f_cp_u21", "type": "int"},
            {"name": "i_cp_u21", "type": "int"},
            {"name": "v_cp_u21", "type": "int"},
            {"name": "p_cp_u21", "type": "int"},
            {"name": "suckt_u21", "type": "int"},
            {"name": "suckp_u21", "type": "int"},
            {"name": "sp_u21", "type": "int"},
            {"name": "eevpos_u21", "type": "int"},
            {"name": "highpress_u21", "type": "int"},
            {"name": "sas_u21", "type": "int"},
            {"name": "ices_u21", "type": "int"},
            {"name": "f_cp_u22", "type": "int"},
            {"name": "i_cp_u22", "type": "int"},
            {"name": "v_cp_u22", "type": "int"},
            {"name": "p_cp_u22", "type": "int"},
            {"name": "suckt_u22", "type": "int"},
            {"name": "suckp_u22", "type": "int"},
            {"name": "sp_u22", "type": "int"},
            {"name": "eevpos_u22", "type": "int"},
            {"name": "highpress_u22", "type": "int"},
            {"name": "sas_u22", "type": "int"},
            {"name": "ices_u22", "type": "int"},
            {"name": "i_ef_u11", "type": "int"},
            {"name": "i_ef_u12", "type": "int"},
            {"name": "i_cf_u11", "type": "int"},
            {"name": "i_cf_u12", "type": "int"},
            {"name": "i_ef_u21", "type": "int"},
            {"name": "i_ef_u22", "type": "int"},
            {"name": "i_cf_u21", "type": "int"},
            {"name": "i_cf_u22", "type": "int"},
            {"name": "i_hvac_u1", "type": "int"},
            {"name": "i_hvac_u2", "type": "int"},
            {"name": "i_exufan", "type": "int"},
            {"name": "dwpower", "type": "int"},
            {"name": "dwemerg_op_tm", "type": "int"},
            {"name": "dwemerg_op_cnt", "type": "int"},
            {"name": "dwef_op_tm_u11", "type": "int"},
            {"name": "dwcf_op_tm_u11", "type": "int"},
            {"name": "dwcp_op_tm_u11", "type": "int"},
            {"name": "dwcp_op_tm_u12", "type": "int"},
            {"name": "dwfad_op_cnt_u1", "type": "int"},
            {"name": "dwrad_op_cnt_u1", "type": "int"},
            {"name": "dwef_op_cnt_u11", "type": "int"},
            {"name": "dwcf_op_cnt_u11", "type": "int"},
            {"name": "dwcp_op_cnt_u11", "type": "int"},
            {"name": "dwcp_op_cnt_u12", "type": "int"},
            {"name": "dwef_op_tm_u21", "type": "int"},
            {"name": "dwcf_op_tm_u21", "type": "int"},
            {"name": "dwcp_op_tm_u21", "type": "int"},
            {"name": "dwcp_op_tm_u22", "type": "int"},
            {"name": "dwfad_op_cnt_u2", "type": "int"},
            {"name": "dwrad_op_cnt_u2", "type": "int"},
            {"name": "dwef_op_cnt_u21", "type": "int"},
            {"name": "dwcf_op_cnt_u21", "type": "int"},
            {"name": "dwcp_op_cnt_u21", "type": "int"},
            {"name": "dwcp_op_cnt_u22", "type": "int"},
            {"name": "dwexufan_op_tm", "type": "int"},
            {"name": "dwexufan_op_cnt", "type": "int"},
            {"name": "dwdmpexu_op_cnt", "type": "int"},
            {"name": "msg_calc_dvc_no", "type": "int"},
            {"name": "msg_calc_train_no", "type": "int"},
            {"name": "msg_calc_dvc_time", "type": "string"},
            {"name": "msg_calc_parse_time", "type": "string"}
        ]
    }

output_schema = faust.Schema(
    key_type=str,
    value_type=ACSignal,
    key_serializer="raw",
    value_serializer="json",
)

output_topic = app.topic(settings.PARSED_TOPIC_NAME, partitions=settings.TOPIC_PARTITIONS, value_serializer='json')