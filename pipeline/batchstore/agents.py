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
from pipeline.batchstore.models import input_topic
from utils.log import log as log
from utils.sensorpolyfit import SensorPolyfit
from utils.tsutil import TSutil

sys_fields_list = [('Flag', 'dvc_flag', 'Basic'),
                    ('车辆编码', 'dvc_train_no', 'Basic'),
                    ('车厢号', 'dvc_carriage_no', 'Basic'),
                    ('年', 'dvc_year', 'Basic'),
                    ('月', 'dvc_month', 'Basic'),
                    ('日', 'dvc_day', 'Basic'),
                    ('时', 'dvc_hour', 'Basic'),
                    ('分', 'dvc_minute', 'Basic'),
                    ('秒', 'dvc_second', 'Basic'),
                    ('列车工况', 'dvc_op_condition', 'Basic'),
                    ('通风机运行U1-1', 'cfbk_ef_u11', 'Status'),
                    ('冷凝风机运行U1-1', 'cfbk_cf_u11', 'Status'),
                    ('冷凝风机运行U1-2', 'cfbk_cf_u12', 'Status'),
                    ('压缩机运行U11', 'cfbk_comp_u11', 'Status'),
                    ('压缩机运行U12', 'cfbk_comp_u12', 'Status'),
                    ('空气净化运行U1-1', 'cfbk_ap_u11', 'Status'),
                    ('空气净化运行U1-2', 'cfbk_ap_u12', 'Status'),
                    ('通风机运行U2-1', 'cfbk_ef_u21', 'Status'),
                    ('冷凝风机运行U2-1', 'cfbk_cf_u21', 'Status'),
                    ('冷凝风机运行U2-2', 'cfbk_cf_u22', 'Status'),
                    ('压缩机运行U21', 'cfbk_comp_u21', 'Status'),
                    ('压缩机运行U22', 'cfbk_comp_u22', 'Status'),
                    ('空气净化运行U2-1', 'cfbk_ap_u21', 'Status'),
                    ('空气净化运行U2-2', 'cfbk_ap_u22', 'Status'),
                    ('机组1三相电源状态', 'cfbk_tpp_u1', 'Status'),
                    ('机组2三相电源状态', 'cfbk_tpp_u2', 'Status'),
                    ('紧急通风运行U1', 'cfbk_ev_u1', 'Status'),
                    ('紧急通风运行U2', 'cfbk_ev_u2', 'Status'),
                    ('通风机过流故障U1-1', 'bocflt_ef_u11', 'Error'),
                    ('通风机过流故障U1-2', 'bocflt_ef_u12', 'Error'),
                    ('冷凝风机过流故障U1-1', 'bocflt_cf_u11', 'Error'),
                    ('冷凝风机过流故障U1-2', 'bocflt_cf_u12', 'Error'),
                    ('变频器保护故障U1-1', 'bflt_vfd_u11', 'Error'),
                    ('变频器通讯故障U1-1', 'bflt_vfd_com_u11', 'Error'),
                    ('变频器保护故障U1-2', 'bflt_vfd_u12', 'Error'),
                    ('变频器通讯故障U1-2', 'bflt_vfd_com_u12', 'Error'),
                    ('低压故障U1-1', 'blpflt_comp_u11', 'Error'),
                    ('高压故障U1-1', 'bscflt_comp_u11', 'Error'),
                    ('排气故障U1-1', 'bscflt_vent_u11', 'Error'),
                    ('低压故障U1-2', 'blpflt_comp_u12', 'Error'),
                    ('高压故障U1-2', 'bscflt_comp_u12', 'Error'),
                    ('排气故障U1-2', 'bscflt_vent_u12', 'Error'),
                    ('新风阀故障U1-1', 'bflt_fad_u11', 'Error'),
                    ('新风阀故障U1-2', 'bflt_fad_u12', 'Error'),
                    ('新风阀故障U1-3', 'bflt_fad_u13', 'Error'),
                    ('新风阀故障U1-4', 'bflt_fad_u14', 'Error'),
                    ('回风阀故障U1-1', 'bflt_rad_u11', 'Error'),
                    ('回风阀故障U1-2', 'bflt_rad_u12', 'Error'),
                    ('回风阀故障U1-3', 'bflt_rad_u13', 'Error'),
                    ('回风阀故障U1-4', 'bflt_rad_u14', 'Error'),
                    ('空气净化器故障U1-1', 'bflt_ap_u11', 'Error'),
                    ('空气净化器故障U1-2', 'bflt_ap_u12', 'Error'),
                    ('扩展模块通讯故障U1', 'bflt_expboard_u1', 'Error'),
                    ('新风温度传感器故障U1', 'bflt_frstemp_u1', 'Error'),
                    ('回风温度传感器故障U1', 'bflt_rnttemp_u1', 'Error'),
                    ('送风温度传感器故障U1-1', 'bflt_splytemp_u11', 'Error'),
                    ('送风温度传感器故障U1-2', 'bflt_splytemp_u12', 'Error'),
                    ('吸气温度传感器故障U1-1', 'bflt_insptemp_u11', 'Error'),
                    ('吸气温度传感器故障U1-2', 'bflt_insptemp_u12', 'Error'),
                    ('低压压力传感器故障U1-1', 'bflt_lowpres_u11', 'Error'),
                    ('低压压力传感器故障U1-2', 'bflt_lowpres_u12', 'Error'),
                    ('高压压力传感器故障U1-1', 'bflt_highpres_u11', 'Error'),
                    ('高压压力传感器故障U1-2', 'bflt_highpres_u12', 'Error'),
                    ('压差传感器故障U1', 'bflt_diffpres_u1', 'Error'),
                    ('通风机过流故障U2-1', 'bocflt_ef_u21', 'Error'),
                    ('通风机过流故障U2-2', 'bocflt_ef_u22', 'Error'),
                    ('冷凝风机过流故障U2-1', 'bocflt_cf_u21', 'Error'),
                    ('冷凝风机过流故障U2-2', 'bocflt_cf_u22', 'Error'),
                    ('变频器保护故障U2-1', 'bflt_vfd_u21', 'Error'),
                    ('变频器通讯故障U2-1', 'bflt_vfd_com_u21', 'Error'),
                    ('变频器保护故障U2-2', 'bflt_vfd_u22', 'Error'),
                    ('变频器通讯故障U2-2', 'bflt_vfd_com_u22', 'Error'),
                    ('低压故障U2-1', 'blpflt_comp_u21', 'Error'),
                    ('高压故障U2-1', 'bscflt_comp_u21', 'Error'),
                    ('排气故障U2-1', 'bscflt_vent_u21', 'Error'),
                    ('低压故障U2-2', 'blpflt_comp_u22', 'Error'),
                    ('高压故障U2-2', 'bscflt_comp_u22', 'Error'),
                    ('排气故障U2-2', 'bscflt_vent_u22', 'Error'),
                    ('新风阀故障U2-1', 'bflt_fad_u21', 'Error'),
                    ('新风阀故障U2-2', 'bflt_fad_u22', 'Error'),
                    ('新风阀故障U2-3', 'bflt_fad_u23', 'Error'),
                    ('新风阀故障U2-4', 'bflt_fad_u24', 'Error'),
                    ('回风阀故障U2-1', 'bflt_rad_u21', 'Error'),
                    ('回风阀故障U2-2', 'bflt_rad_u22', 'Error'),
                    ('回风阀故障U2-3', 'bflt_rad_u23', 'Error'),
                    ('回风阀故障U2-4', 'bflt_rad_u24', 'Error'),
                    ('空气净化器故障U2-1', 'bflt_ap_u21', 'Error'),
                    ('空气净化器故障U2-2', 'bflt_ap_u22', 'Error'),
                    ('扩展模块通讯故障U2', 'bflt_expboard_u2', 'Error'),
                    ('新风温度传感器故障U2', 'bflt_frstemp_u2', 'Error'),
                    ('回风温度传感器故障U2', 'bflt_rnttemp_u2', 'Error'),
                    ('送风温度传感器故障U2-1', 'bflt_splytemp_u21', 'Error'),
                    ('送风温度传感器故障U2-2', 'bflt_splytemp_u22', 'Error'),
                    ('吸气温度传感器故障U2-1', 'bflt_insptemp_u21', 'Error'),
                    ('吸气温度传感器故障U2-2', 'bflt_insptemp_u22', 'Error'),
                    ('低压压力传感器故障U2-1', 'bflt_lowpres_u21', 'Error'),
                    ('低压压力传感器故障U2-2', 'bflt_lowpres_u22', 'Error'),
                    ('高压压力传感器故障U2-1', 'bflt_highpres_u21', 'Error'),
                    ('高压压力传感器故障U2-2', 'bflt_highpres_u22', 'Error'),
                    ('压差传感器故障U2', 'bflt_diffpres_u2', 'Error'),
                    ('紧急逆变器故障', 'bflt_emergivt', 'Error'),
                    ('车厢温度传感器1故障', 'bflt_vehtemp_u1', 'Error'),
                    ('车厢湿度传感器1故障', 'bflt_vehhum_u1', 'Error'),
                    ('车厢温度传感器2故障', 'bflt_vehtemp_u2', 'Error'),
                    ('车厢湿度传感器2故障', 'bflt_vehhum_u2', 'Error'),
                    ('机组1空气监测终端通讯故障', 'bflt_airmon_u1', 'Error'),
                    ('机组2空气监测终端通讯故障', 'bflt_airmon_u2', 'Error'),
                    ('电流监测单元通讯故障', 'bflt_currentmon', 'Error'),
                    ('TCMS通讯故障', 'bflt_tcms', 'Error'),
                    ('通风机短路故障U1', 'bscflt_ef_u1', 'Error'),
                    ('冷凝风机短路故障U1', 'bscflt_cf_u1', 'Error'),
                    ('变频器供电短路故障U1-1', 'bscflt_vfd_pw_u11', 'Error'),
                    ('变频器供电短路故障U1-2', 'bscflt_vfd_pw_u12', 'Error'),
                    ('通风机短路故障U2', 'bscflt_ef_u2', 'Error'),
                    ('冷凝风机短路故障U2', 'bscflt_cf_u2', 'Error'),
                    ('变频器供电短路故障U2-1', 'bscflt_vfd_pw_u21', 'Error'),
                    ('变频器供电短路故障U2-2', 'bscflt_vfd_pw_u22', 'Error'),
                    ('通风机接触器故障U1', 'bflt_ef_cnt_u1', 'Error'),
                    ('冷凝风机接触器故障U1-1', 'bflt_cf_cnt_u11', 'Error'),
                    ('冷凝风机接触器故障U1-2', 'bflt_cf_cnt_u12', 'Error'),
                    ('压缩机变频器接触器故障U1-1', 'bflt_vfd_cnt_u11', 'Error'),
                    ('压缩机变频器接触器故障U1-2', 'bflt_vfd_cnt_u12', 'Error'),
                    ('紧急通风接触器故障U1', 'bflt__ev_cnt_u1', 'Error'),
                    ('通风机接触器故障U2', 'bflt_ef_cnt_u2', 'Error'),
                    ('冷凝风机接触器故障U2-1', 'bflt_cf_cnt_u21', 'Error'),
                    ('冷凝风机接触器故障U2-2', 'bflt_cf_cnt_u22', 'Error'),
                    ('压缩机变频器接触器故障U2-1', 'bflt_vfd_cnt_u21', 'Error'),
                    ('压缩机变频器接触器故障U2-2', 'bflt_vfd_cnt_u22', 'Error'),
                    ('紧急通风接触器故障U2', 'bflt__ev_cnt_u2', 'Error'),
                    ('车厢温度超温预警', 'bflt_tempover', 'Error'),
                    ('新风温度-系统', 'fas_sys', 'Param'),
                    ('回风温度-系统', 'ras_sys', 'Param'),
                    ('目标温度', 'tic', 'Param'),
                    ('载客量', 'load', 'Param'),
                    ('车厢温度-1', 'tveh_1', 'Param'),
                    ('车厢湿度-1', 'humdity_1', 'Param'),
                    ('车厢温度-2', 'tveh_2', 'Param'),
                    ('车厢湿度-2', 'humdity_2', 'Param'),
                    ('空气质量-温度-U1', 'aq_t_u1', 'Param'),
                    ('空气质量-湿度-U1', 'aq_h_u1', 'Param'),
                    ('空气质量-CO2-U1', 'aq_co2_u1', 'Param'),
                    ('空气质量-TVOC-U1', 'aq_tvoc_u1', 'Param'),
                    ('空气质量-PM2.5-U1', 'aq_pm2_5_u1', 'Param'),
                    ('空气质量-PM10-U1', 'aq_pm10_u1', 'Param'),
                    ('空调运行模式U1', 'wmode_u1', 'Param'),
                    ('压差-U1', 'presdiff_u1', 'Param'),
                    ('新风温度-U1', 'fas_u1', 'Param'),
                    ('回风温度-U1', 'ras_u1', 'Param'),
                    ('新风阀开度-U1', 'fadpos_u1', 'Param'),
                    ('回风阀开度-U1', 'radpos_u1', 'Param'),
                    ('压缩机频率-U11', 'f_cp_u11', 'Param'),
                    ('压缩机电流-U11', 'i_cp_u11', 'Param'),
                    ('压缩机电压-U11', 'v_cp_u11', 'Param'),
                    ('压缩机功率-U11', 'p_cp_u11', 'Param'),
                    ('吸气温度-U11', 'suckt_u11', 'Param'),
                    ('吸气压力-U11', 'suckp_u11', 'Param'),
                    ('过热度-U11', 'sp_u11', 'Param'),
                    ('电子膨胀阀开度-U11', 'eevpos_u11', 'Param'),
                    ('高压压力-U11', 'highpress_u11', 'Param'),
                    ('送风温度-U11', 'sas_u11', 'Param'),
                    ('压缩机频率-U12', 'f_cp_u12', 'Param'),
                    ('压缩机电流-U12', 'i_cp_u12', 'Param'),
                    ('压缩机电压-U12', 'v_cp_u12', 'Param'),
                    ('压缩机功率-U12', 'p_cp_u12', 'Param'),
                    ('吸气温度-U12', 'suckt_u12', 'Param'),
                    ('吸气压力-U12', 'suckp_u12', 'Param'),
                    ('过热度-U12', 'sp_u12', 'Param'),
                    ('电子膨胀阀开度-U12', 'eevpos_u12', 'Param'),
                    ('高压压力-U12', 'highpress_u12', 'Param'),
                    ('送风温度-U12', 'sas_u12', 'Param'),
                    ('空气质量-温度-U2', 'aq_t_u2', 'Param'),
                    ('空气质量-湿度-U2', 'aq_h_u2', 'Param'),
                    ('空气质量-CO2-U2', 'aq_co2_u2', 'Param'),
                    ('空气质量-TVOC-U2', 'aq_tvoc_u2', 'Param'),
                    ('空气质量-PM2.5-U2', 'aq_pm2_5_u2', 'Param'),
                    ('空气质量-PM10-U2', 'aq_pm10_u2', 'Param'),
                    ('空调运行模式U2', 'wmode_u2', 'Param'),
                    ('压差-U2', 'presdiff_u2', 'Param'),
                    ('新风温度-U2', 'fas_u2', 'Param'),
                    ('回风温度-U2', 'ras_u2', 'Param'),
                    ('新风阀开度-U2', 'fadpos_u2', 'Param'),
                    ('回风阀开度-U2', 'radpos_u2', 'Param'),
                    ('压缩机频率-U21', 'f_cp_u21', 'Param'),
                    ('压缩机电流-U21', 'i_cp_u21', 'Param'),
                    ('压缩机电压-U21', 'v_cp_u21', 'Param'),
                    ('压缩机功率-U21', 'p_cp_u21', 'Param'),
                    ('吸气温度-U21', 'suckt_u21', 'Param'),
                    ('吸气压力-U21', 'suckp_u21', 'Param'),
                    ('过热度-U21', 'sp_u21', 'Param'),
                    ('电子膨胀阀开度-U21', 'eevpos_u21', 'Param'),
                    ('高压压力-U21', 'highpress_u21', 'Param'),
                    ('送风温度-U21', 'sas_u21', 'Param'),
                    ('压缩机频率-U22', 'f_cp_u22', 'Param'),
                    ('压缩机电流-U22', 'i_cp_u22', 'Param'),
                    ('压缩机电压-U22', 'v_cp_u22', 'Param'),
                    ('压缩机功率-U22', 'p_cp_u22', 'Param'),
                    ('吸气温度-U22', 'suckt_u22', 'Param'),
                    ('吸气压力-U22', 'suckp_u22', 'Param'),
                    ('过热度-U22', 'sp_u22', 'Param'),
                    ('电子膨胀阀开度-U22', 'eevpos_u22', 'Param'),
                    ('高压压力-U22', 'highpress_u22', 'Param'),
                    ('送风温度-U22', 'sas_u22', 'Param'),
                    ('通风机电流-U11', 'i_ef_u11', 'Param'),
                    ('通风机电流-U12', 'i_ef_u12', 'Param'),
                    ('冷凝风机电流-U11', 'i_cf_u11', 'Param'),
                    ('冷凝风机电流-U12', 'i_cf_u12', 'Param'),
                    ('通风机电流-U21', 'i_ef_u21', 'Param'),
                    ('通风机电流-U22', 'i_ef_u22', 'Param'),
                    ('冷凝风机电流-U21', 'i_cf_u21', 'Param'),
                    ('冷凝风机电流-U22', 'i_cf_u22', 'Param'),
                    ('总电流-机组1', 'i_hvac_u1', 'Param'),
                    ('总电流-机组2', 'i_hvac_u2', 'Param'),
                    ('空调机组能耗', 'dwpower', 'Param'),
                    ('紧急逆变器累计运行时间', 'dwemerg_op_tm', 'Statistics'),
                    ('紧急逆变器累计运行次数', 'dwemerg_op_cnt', 'Statistics'),
                    ('通风机累计运行时间-U11', 'dwef_op_tm_u11', 'Statistics'),
                    ('冷凝风机累计运行时间-U11', 'dwcf_op_tm_u11', 'Statistics'),
                    ('冷凝风机累计运行时间-U12', 'dwcf_op_tm_u12', 'Statistics'),
                    ('压缩机累计运行时间-U11', 'dwcp_op_tm_u11', 'Statistics'),
                    ('压缩机累计运行时间-U12', 'dwcp_op_tm_u12', 'Statistics'),
                    ('空气净化器累计运行时间-U11', 'dwap_op_tm_u11', 'Statistics'),
                    ('空气净化器累计运行时间-U12', 'dwap_op_tm_u12', 'Statistics'),
                    ('新风阀开关次数-U1', 'dwfad_op_cnt_u1', 'Statistics'),
                    ('回风阀开关次数-U1', 'dwrad_op_cnt_u1', 'Statistics'),
                    ('通风机累计开关次数-U11', 'dwef_op_cnt_u11', 'Statistics'),
                    ('冷凝风机累计开关次数-U11', 'dwcf_op_cnt_u11', 'Statistics'),
                    ('冷凝风机累计开关次数-U12', 'dwcf_op_cnt_u12', 'Statistics'),
                    ('压缩机累计开关次数-U11', 'dwcp_op_cnt_u11', 'Statistics'),
                    ('压缩机累计开关次数-U12', 'dwcp_op_cnt_u12', 'Statistics'),
                    ('空气净化器累计开关次数-U11', 'dwap_op_cnt_u11', 'Statistics'),
                    ('空气净化器累计开关次数-U12', 'dwap_op_cnt_u12', 'Statistics'),
                    ('通风机累计运行时间-U21', 'dwef_op_tm_u21', 'Statistics'),
                    ('冷凝风机累计运行时间-U21', 'dwcf_op_tm_u21', 'Statistics'),
                    ('冷凝风机累计运行时间-U22', 'dwcf_op_tm_u22', 'Statistics'),
                    ('压缩机累计运行时间-U21', 'dwcp_op_tm_u21', 'Statistics'),
                    ('压缩机累计运行时间-U22', 'dwcp_op_tm_u22', 'Statistics'),
                    ('空气净化器累计运行时间-U21', 'dwap_op_tm_u21', 'Statistics'),
                    ('空气净化器累计运行时间-U22', 'dwap_op_tm_u22', 'Statistics'),
                    ('新风阀开关次数-U2', 'dwfad_op_cnt_u2', 'Statistics'),
                    ('回风阀开关次数-U2', 'dwrad_op_cnt_u2', 'Statistics'),
                    ('通风机累计开关次数-U21', 'dwef_op_cnt_u21', 'Statistics'),
                    ('冷凝风机累计开关次数-U21', 'dwcf_op_cnt_u21', 'Statistics'),
                    ('冷凝风机累计开关次数-U22', 'dwcf_op_cnt_u22', 'Statistics'),
                    ('压缩机累计开关次数-U21', 'dwcp_op_cnt_u21', 'Statistics'),
                    ('压缩机累计开关次数-U22', 'dwcp_op_cnt_u22', 'Statistics'),
                    ('空气净化器累计开关次数-U21', 'dwap_op_cnt_u21', 'Statistics'),
                    ('空气净化器累计开关次数-U22', 'dwap_op_cnt_u22', 'Statistics'),
                    ('冷媒泄露预警 U-11', 'ref_leak_u11', 'predict'),
                    ('冷媒泄露预警 U-12', 'ref_leak_u12', 'predict'),
                    ('冷媒泄露预警 U-21', 'ref_leak_u21', 'predict'),
                    ('冷媒泄露预警 U-22', 'ref_leak_u22', 'predict'),
                    ('制冷系统预警 U-1', 'f_cp_u1', 'predict'),
                    ('制冷系统预警 U-2', 'f_cp_u2', 'predict'),
                    ('新风温度传感器预警', 'f_fas', 'predict'),
                    ('回风温度传感器预警', 'f_ras', 'predict'),
                    ('车厢温度超温预警', 'cabin_overtemp', 'predict'),
                    ('滤网脏堵预警 U-1', 'f_presdiff_u1', 'predict'),
                    ('滤网脏堵预警 U-2', 'f_presdiff_u2', 'predict'),
                    ('通风机电流预警 U-11', 'f_ef_u11', 'predict'),
                    ('通风机电流预警 U-12', 'f_ef_u12', 'predict'),
                    ('通风机电流预警 U-21', 'f_ef_u21', 'predict'),
                    ('通风机电流预警 U-22', 'f_ef_u22', 'predict'),
                    ('冷凝风机电流预警 U-11', 'f_cf_u11', 'predict'),
                    ('冷凝风机电流预警 U-12', 'f_cf_u12', 'predict'),
                    ('冷凝风机电流预警 U-21', 'f_cf_u21', 'predict'),
                    ('冷凝风机电流预警 U-22', 'f_cf_u22', 'predict'),
                    ('压缩机电流预警 U-11', 'f_fas_u11', 'predict'),
                    ('压缩机电流预警 U-12', 'f_fas_u12', 'predict'),
                    ('压缩机电流预警 U-21', 'f_fas_u21', 'predict'),
                    ('压缩机电流预警 U-22', 'f_fas_u22', 'predict'),
                    ('空气质量监测终端预警 U-1', 'f_aq_u1', 'predict'),
                    ('空气质量监测终端预警 U-2', 'f_aq_u2', 'predict')]
@app.agent(input_topic)
async def store_signal(stream):
    tu = TSutil()
    async for datas in stream.take(settings.TSDB_BATCH, within=settings.TSDB_BATCH_TIME):
        log.debug("==================== Get parsed data batch ====================")

        # 创建参数代码到名称的映射
        code_to_name = {code: name for name, code, category in sys_fields_list}
        # 按类别分组参数代码
        category_to_codes = {}
        for name, code, category in sys_fields_list:
            if category.lower() != 'basic':
                category_to_codes.setdefault(category.lower(), []).append(code)
        # 按类别准备数据
        categories = ['status', 'param', 'error', 'statistics']
        category_data = {cat: [] for cat in categories}
        # 创建类别映射表（优化：直接映射小写类别到目标键）
        category_mapping = {
            'status': 'status',
            'param': 'param',
            'error': 'error',
            'statistics': 'statistics'
        }

        for data in datas:
            payload = data.get('payload', {})

            # 提取关键信息（优化：提前提取并验证，减少重复访问）
            msg_calc_dvc_time = payload.get('msg_calc_dvc_time')
            msg_calc_parse_time = payload.get('msg_calc_parse_time')

            # 跳过无效记录（优化：提前退出，减少后续处理）
            if not msg_calc_dvc_time or not msg_calc_parse_time:
                continue

            # 提取设备信息（优化：一次性提取，减少payload访问）
            device_info = {
                'msg_calc_dvc_time': msg_calc_dvc_time,
                'msg_calc_parse_time': msg_calc_parse_time,
                'msg_calc_dvc_no': payload.get('msg_calc_dvc_no', ''),
                'msg_calc_train_no': payload.get('msg_calc_train_no', ''),
                'dvc_train_no': payload.get('dvc_train_no'),
                'dvc_carriage_no': payload.get('dvc_carriage_no')
            }

            # 为每个类别创建数据记录（优化：减少嵌套层级）
            for category, codes in category_to_codes.items():
                target_category = category_mapping.get(category)

                # 跳过未定义的类别
                if target_category not in category_data:
                    continue

                # 生成记录（优化：使用列表推导式生成多条记录）
                records = [
                    {
                        'payload': {
                            **device_info,  # 使用展开操作符合并设备信息
                            'param_name': code_to_name.get(code, code),
                            'param_value': payload[code]
                        }
                    }
                    for code in codes
                    if code in payload
                ]

                # 添加到结果列表（优化：批量扩展列表，减少append调用）
                if records:
                    category_data[target_category].extend(records)
        log.debug(len(category_data))
        log.debug(len(category_data['status']))
        log.debug(len(category_data['param']))
        log.debug(len(category_data['error']))
        log.debug(len(category_data['statistics']))

        dev_mode = settings.DEV_MODE
        if dev_mode:
            tu.batchinsert('dev_macda', 'msg_calc_parse_time', datas)
            tu.batchinsert('dev_status_transposed', 'msg_calc_parse_time', category_data['status'])
            tu.batchinsert('dev_param_transposed', 'msg_calc_parse_time', category_data['param'])
            tu.batchinsert('dev_error_transposed', 'msg_calc_parse_time', category_data['error'])
            tu.batchinsert('dev_statistics_transposed', 'msg_calc_parse_time', category_data['statistics'])
        else:
            tu.batchinsert('pro_macda', 'msg_calc_dvc_time', datas)
            tu.batchinsert('pro_status_transposed', 'msg_calc_dvc_time', category_data['status'])
            tu.batchinsert('pro_param_transposed', 'msg_calc_dvc_time', category_data['param'])
            tu.batchinsert('pro_error_transposed', 'msg_calc_dvc_time', category_data['error'])
            tu.batchinsert('pro_statistics_transposed', 'msg_calc_dvc_time', category_data['statistics'])
        log.debug("Saved data with batch length: %s" % len(datas))

'''
    async for data in stream:
        #log.debug(data['payload'])
        datalist = []
        if len(datalist) < 50:
            if 'payload' in data:
                datalist.append(data['payload'])
                log.debug(
                    "Saved data with key : (%s: %s)" % (len(datalist),f"{data['payload']['msg_calc_dvc_no']}-{data['payload']['msg_calc_dvc_time']}"))
        else:
            tu.batchinsert('pro_macda',datalist)
            tu.batchinsert('dev_macda',datalist)
        #else:
        #    tu.insert('pro_macda', data)
        #    tu.insert('dev_macda', data)
        #    log.debug("Saved data with key : %s" % f"{data['msg_calc_dvc_no']}-{data['msg_calc_dvc_time']}")
    '''