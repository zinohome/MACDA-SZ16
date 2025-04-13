#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import traceback
import weakref
import psycopg2
from datetime import datetime
from pgcopy import CopyManager
from psycopg2 import pool
from core.settings import settings
from utils.log import log as log
from collections import Counter
import simplejson as json


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
class TSutil(metaclass=Cached):
    def __init__(self):
        log.debug('Connect to timescaledb uri [ %s ]' % settings.TSDB_URL)
        self.conn_pool = psycopg2.pool.SimpleConnectionPool(1, settings.TSDB_POOL_SIZE, settings.TSDB_URL)
        if (self.conn_pool):
            log.debug("Connection pool created successfully")
        try:
            log.debug("Check tsdb table ...")
            conn = self.conn_pool.getconn()
            #create_pro_table = "CREATE TABLE IF NOT EXISTS pro_macda (msg_calc_dvc_time TIMESTAMPTZ NOT NULL,msg_calc_parse_time TEXT NOT NULL,msg_calc_dvc_no TEXT NOT NULL,msg_calc_train_no TEXT NOT NULL,msg_header_code01 DOUBLE PRECISION NULL,msg_header_code02 DOUBLE PRECISION NULL,msg_length DOUBLE PRECISION NULL,msg_src_dvc_no DOUBLE PRECISION NULL,msg_host_dvc_no DOUBLE PRECISION NULL,msg_type DOUBLE PRECISION NULL,msg_frame_no DOUBLE PRECISION NULL,msg_line_no DOUBLE PRECISION NULL,msg_train_type DOUBLE PRECISION NULL,msg_train_no DOUBLE PRECISION NULL,msg_carriage_no DOUBLE PRECISION NULL,msg_protocal_version DOUBLE PRECISION NULL,dvc_flag DOUBLE PRECISION NULL,dvc_train_no DOUBLE PRECISION NULL,dvc_carriage_no DOUBLE PRECISION NULL,dvc_year DOUBLE PRECISION NULL,dvc_month DOUBLE PRECISION NULL,dvc_day DOUBLE PRECISION NULL,dvc_hour DOUBLE PRECISION NULL,dvc_minute DOUBLE PRECISION NULL,dvc_second DOUBLE PRECISION NULL,cfbk_ef_u11 DOUBLE PRECISION NULL,cfbk_cf_u11 DOUBLE PRECISION NULL,cfbk_comp_u11 DOUBLE PRECISION NULL,cfbk_comp_u12 DOUBLE PRECISION NULL,cfbk_ap_u11 DOUBLE PRECISION NULL,cfbk_ef_u21 DOUBLE PRECISION NULL,cfbk_cf_u21 DOUBLE PRECISION NULL,cfbk_comp_u21 DOUBLE PRECISION NULL,cfbk_comp_u22 DOUBLE PRECISION NULL,cfbk_ap_u21 DOUBLE PRECISION NULL,cfbk_tpp_u1 DOUBLE PRECISION NULL,cfbk_tpp_u2 DOUBLE PRECISION NULL,cfbk_ev_u1 DOUBLE PRECISION NULL,cfbk_ev_u2 DOUBLE PRECISION NULL,cfbk_ewd DOUBLE PRECISION NULL,cfbk_exufan DOUBLE PRECISION NULL,bocflt_ef_u11 DOUBLE PRECISION NULL,bocflt_ef_u12 DOUBLE PRECISION NULL,bocflt_cf_u11 DOUBLE PRECISION NULL,bocflt_cf_u12 DOUBLE PRECISION NULL,bflt_vfd_u11 DOUBLE PRECISION NULL,bflt_vfd_com_u11 DOUBLE PRECISION NULL,bflt_vfd_u12 DOUBLE PRECISION NULL,bflt_vfd_com_u12 DOUBLE PRECISION NULL,blpflt_comp_u11 DOUBLE PRECISION NULL,bscflt_comp_u11 DOUBLE PRECISION NULL,bscflt_vent_u11 DOUBLE PRECISION NULL,blpflt_comp_u12 DOUBLE PRECISION NULL,bscflt_comp_u12 DOUBLE PRECISION NULL,bscflt_vent_u12 DOUBLE PRECISION NULL,bflt_fad_u11 DOUBLE PRECISION NULL,bflt_fad_u12 DOUBLE PRECISION NULL,bflt_rad_u11 DOUBLE PRECISION NULL,bflt_rad_u12 DOUBLE PRECISION NULL,bflt_ap_u11 DOUBLE PRECISION NULL,bflt_expboard_u1 DOUBLE PRECISION NULL,bflt_frstemp_u1 DOUBLE PRECISION NULL,bflt_rnttemp_u1 DOUBLE PRECISION NULL,bflt_splytemp_u11 DOUBLE PRECISION NULL,bflt_splytemp_u12 DOUBLE PRECISION NULL,bflt_coiltemp_u11 DOUBLE PRECISION NULL,bflt_coiltemp_u12 DOUBLE PRECISION NULL,bflt_insptemp_u11 DOUBLE PRECISION NULL,bflt_insptemp_u12 DOUBLE PRECISION NULL,bflt_lowpres_u11 DOUBLE PRECISION NULL,bflt_lowpres_u12 DOUBLE PRECISION NULL,bflt_highpres_u11 DOUBLE PRECISION NULL,bflt_highpres_u12 DOUBLE PRECISION NULL,bflt_diffpres_u1 DOUBLE PRECISION NULL,bocflt_ef_u21 DOUBLE PRECISION NULL,bocflt_ef_u22 DOUBLE PRECISION NULL,bocflt_cf_u21 DOUBLE PRECISION NULL,bocflt_cf_u22 DOUBLE PRECISION NULL,bflt_vfd_u21 DOUBLE PRECISION NULL,bflt_vfd_com_u21 DOUBLE PRECISION NULL,bflt_vfd_u22 DOUBLE PRECISION NULL,bflt_vfd_com_u22 DOUBLE PRECISION NULL,blpflt_comp_u21 DOUBLE PRECISION NULL,bscflt_comp_u21 DOUBLE PRECISION NULL,bscflt_vent_u21 DOUBLE PRECISION NULL,blpflt_comp_u22 DOUBLE PRECISION NULL,bscflt_comp_u22 DOUBLE PRECISION NULL,bscflt_vent_u22 DOUBLE PRECISION NULL,bflt_fad_u21 DOUBLE PRECISION NULL,bflt_fad_u22 DOUBLE PRECISION NULL,bflt_rad_u21 DOUBLE PRECISION NULL,bflt_rad_u22 DOUBLE PRECISION NULL,bflt_ap_u21 DOUBLE PRECISION NULL,bflt_expboard_u2 DOUBLE PRECISION NULL,bflt_frstemp_u2 DOUBLE PRECISION NULL,bflt_rnttemp_u2 DOUBLE PRECISION NULL,bflt_splytemp_u21 DOUBLE PRECISION NULL,bflt_splytemp_u22 DOUBLE PRECISION NULL,bflt_coiltemp_u21 DOUBLE PRECISION NULL,bflt_coiltemp_u22 DOUBLE PRECISION NULL,bflt_insptemp_u21 DOUBLE PRECISION NULL,bflt_insptemp_u22 DOUBLE PRECISION NULL,bflt_lowpres_u21 DOUBLE PRECISION NULL,bflt_lowpres_u22 DOUBLE PRECISION NULL,bflt_highpres_u21 DOUBLE PRECISION NULL,bflt_highpres_u22 DOUBLE PRECISION NULL,bflt_diffpres_u2 DOUBLE PRECISION NULL,bflt_emergivt DOUBLE PRECISION NULL,bflt_vehtemp_u1 DOUBLE PRECISION NULL,bflt_vehtemp_u2 DOUBLE PRECISION NULL,bflt_airmon_u1 DOUBLE PRECISION NULL,bflt_airmon_u2 DOUBLE PRECISION NULL,bflt_currentmon DOUBLE PRECISION NULL,bflt_tcms DOUBLE PRECISION NULL,bflt_tempover DOUBLE PRECISION NULL,bflt_powersupply_u1 DOUBLE PRECISION NULL,bflt_powersupply_u2 DOUBLE PRECISION NULL,bflt_exhaustfan DOUBLE PRECISION NULL,bflt_exhaustval DOUBLE PRECISION NULL,fas_sys DOUBLE PRECISION NULL,ras_sys DOUBLE PRECISION NULL,tic DOUBLE PRECISION NULL,load DOUBLE PRECISION NULL,tveh_1 DOUBLE PRECISION NULL,tveh_2 DOUBLE PRECISION NULL,aq_t_u1 DOUBLE PRECISION NULL,aq_h_u1 DOUBLE PRECISION NULL,aq_co2_u1 DOUBLE PRECISION NULL,aq_tvoc_u1 DOUBLE PRECISION NULL,aq_formald_u1 DOUBLE PRECISION NULL,aq_pm2_5_u1 DOUBLE PRECISION NULL,aq_pm10_u1 DOUBLE PRECISION NULL,wmode_u1 DOUBLE PRECISION NULL,presdiff_u1 DOUBLE PRECISION NULL,fas_u1 DOUBLE PRECISION NULL,ras_u1 DOUBLE PRECISION NULL,fadpos_u1 DOUBLE PRECISION NULL,radpos_u1 DOUBLE PRECISION NULL,f_cp_u11 DOUBLE PRECISION NULL,i_cp_u11 DOUBLE PRECISION NULL,v_cp_u11 DOUBLE PRECISION NULL,p_cp_u11 DOUBLE PRECISION NULL,suckt_u11 DOUBLE PRECISION NULL,suckp_u11 DOUBLE PRECISION NULL,sp_u11 DOUBLE PRECISION NULL,eevpos_u11 DOUBLE PRECISION NULL,highpress_u11 DOUBLE PRECISION NULL,sas_u11 DOUBLE PRECISION NULL,ices_u11 DOUBLE PRECISION NULL,f_cp_u12 DOUBLE PRECISION NULL,i_cp_u12 DOUBLE PRECISION NULL,v_cp_u12 DOUBLE PRECISION NULL,p_cp_u12 DOUBLE PRECISION NULL,suckt_u12 DOUBLE PRECISION NULL,suckp_u12 DOUBLE PRECISION NULL,sp_u12 DOUBLE PRECISION NULL,eevpos_u12 DOUBLE PRECISION NULL,highpress_u12 DOUBLE PRECISION NULL,sas_u12 DOUBLE PRECISION NULL,ices_u12 DOUBLE PRECISION NULL,aq_t_u2 DOUBLE PRECISION NULL,aq_h_u2 DOUBLE PRECISION NULL,aq_co2_u2 DOUBLE PRECISION NULL,aq_tvoc_u2 DOUBLE PRECISION NULL,aq_formald_u2 DOUBLE PRECISION NULL,aq_pm2_5_u2 DOUBLE PRECISION NULL,aq_pm10_u2 DOUBLE PRECISION NULL,wmode_u2 DOUBLE PRECISION NULL,presdiff_u2 DOUBLE PRECISION NULL,fas_u2 DOUBLE PRECISION NULL,ras_u2 DOUBLE PRECISION NULL,fadpos_u2 DOUBLE PRECISION NULL,radpos_u2 DOUBLE PRECISION NULL,f_cp_u21 DOUBLE PRECISION NULL,i_cp_u21 DOUBLE PRECISION NULL,v_cp_u21 DOUBLE PRECISION NULL,p_cp_u21 DOUBLE PRECISION NULL,suckt_u21 DOUBLE PRECISION NULL,suckp_u21 DOUBLE PRECISION NULL,sp_u21 DOUBLE PRECISION NULL,eevpos_u21 DOUBLE PRECISION NULL,highpress_u21 DOUBLE PRECISION NULL,sas_u21 DOUBLE PRECISION NULL,ices_u21 DOUBLE PRECISION NULL,f_cp_u22 DOUBLE PRECISION NULL,i_cp_u22 DOUBLE PRECISION NULL,v_cp_u22 DOUBLE PRECISION NULL,p_cp_u22 DOUBLE PRECISION NULL,suckt_u22 DOUBLE PRECISION NULL,suckp_u22 DOUBLE PRECISION NULL,sp_u22 DOUBLE PRECISION NULL,eevpos_u22 DOUBLE PRECISION NULL,highpress_u22 DOUBLE PRECISION NULL,sas_u22 DOUBLE PRECISION NULL,ices_u22 DOUBLE PRECISION NULL,i_ef_u11 DOUBLE PRECISION NULL,i_ef_u12 DOUBLE PRECISION NULL,i_cf_u11 DOUBLE PRECISION NULL,i_cf_u12 DOUBLE PRECISION NULL,i_ef_u21 DOUBLE PRECISION NULL,i_ef_u22 DOUBLE PRECISION NULL,i_cf_u21 DOUBLE PRECISION NULL,i_cf_u22 DOUBLE PRECISION NULL,i_hvac_u1 DOUBLE PRECISION NULL,i_hvac_u2 DOUBLE PRECISION NULL,i_exufan DOUBLE PRECISION NULL,dwpower DOUBLE PRECISION NULL,dwemerg_op_tm DOUBLE PRECISION NULL,dwemerg_op_cnt DOUBLE PRECISION NULL,dwef_op_tm_u11 DOUBLE PRECISION NULL,dwcf_op_tm_u11 DOUBLE PRECISION NULL,dwcp_op_tm_u11 DOUBLE PRECISION NULL,dwcp_op_tm_u12 DOUBLE PRECISION NULL,dwfad_op_cnt_u1 DOUBLE PRECISION NULL,dwrad_op_cnt_u1 DOUBLE PRECISION NULL,dwef_op_cnt_u11 DOUBLE PRECISION NULL,dwcf_op_cnt_u11 DOUBLE PRECISION NULL,dwcp_op_cnt_u11 DOUBLE PRECISION NULL,dwcp_op_cnt_u12 DOUBLE PRECISION NULL,dwef_op_tm_u21 DOUBLE PRECISION NULL,dwcf_op_tm_u21 DOUBLE PRECISION NULL,dwcp_op_tm_u21 DOUBLE PRECISION NULL,dwcp_op_tm_u22 DOUBLE PRECISION NULL,dwfad_op_cnt_u2 DOUBLE PRECISION NULL,dwrad_op_cnt_u2 DOUBLE PRECISION NULL,dwef_op_cnt_u21 DOUBLE PRECISION NULL,dwcf_op_cnt_u21 DOUBLE PRECISION NULL,dwcp_op_cnt_u21 DOUBLE PRECISION NULL,dwcp_op_cnt_u22 DOUBLE PRECISION NULL,dwexufan_op_tm DOUBLE PRECISION NULL,dwexufan_op_cnt DOUBLE PRECISION NULL,dwdmpexu_op_cnt DOUBLE PRECISION NULL);"
            #create_dev_table = "CREATE TABLE IF NOT EXISTS dev_macda (msg_calc_dvc_time TEXT NOT NULL,msg_calc_parse_time TIMESTAMPTZ NOT NULL,msg_calc_dvc_no TEXT NOT NULL,msg_calc_train_no TEXT NOT NULL,msg_header_code01 DOUBLE PRECISION NULL,msg_header_code02 DOUBLE PRECISION NULL,msg_length DOUBLE PRECISION NULL,msg_src_dvc_no DOUBLE PRECISION NULL,msg_host_dvc_no DOUBLE PRECISION NULL,msg_type DOUBLE PRECISION NULL,msg_frame_no DOUBLE PRECISION NULL,msg_line_no DOUBLE PRECISION NULL,msg_train_type DOUBLE PRECISION NULL,msg_train_no DOUBLE PRECISION NULL,msg_carriage_no DOUBLE PRECISION NULL,msg_protocal_version DOUBLE PRECISION NULL,dvc_flag DOUBLE PRECISION NULL,dvc_train_no DOUBLE PRECISION NULL,dvc_carriage_no DOUBLE PRECISION NULL,dvc_year DOUBLE PRECISION NULL,dvc_month DOUBLE PRECISION NULL,dvc_day DOUBLE PRECISION NULL,dvc_hour DOUBLE PRECISION NULL,dvc_minute DOUBLE PRECISION NULL,dvc_second DOUBLE PRECISION NULL,cfbk_ef_u11 DOUBLE PRECISION NULL,cfbk_cf_u11 DOUBLE PRECISION NULL,cfbk_comp_u11 DOUBLE PRECISION NULL,cfbk_comp_u12 DOUBLE PRECISION NULL,cfbk_ap_u11 DOUBLE PRECISION NULL,cfbk_ef_u21 DOUBLE PRECISION NULL,cfbk_cf_u21 DOUBLE PRECISION NULL,cfbk_comp_u21 DOUBLE PRECISION NULL,cfbk_comp_u22 DOUBLE PRECISION NULL,cfbk_ap_u21 DOUBLE PRECISION NULL,cfbk_tpp_u1 DOUBLE PRECISION NULL,cfbk_tpp_u2 DOUBLE PRECISION NULL,cfbk_ev_u1 DOUBLE PRECISION NULL,cfbk_ev_u2 DOUBLE PRECISION NULL,cfbk_ewd DOUBLE PRECISION NULL,cfbk_exufan DOUBLE PRECISION NULL,bocflt_ef_u11 DOUBLE PRECISION NULL,bocflt_ef_u12 DOUBLE PRECISION NULL,bocflt_cf_u11 DOUBLE PRECISION NULL,bocflt_cf_u12 DOUBLE PRECISION NULL,bflt_vfd_u11 DOUBLE PRECISION NULL,bflt_vfd_com_u11 DOUBLE PRECISION NULL,bflt_vfd_u12 DOUBLE PRECISION NULL,bflt_vfd_com_u12 DOUBLE PRECISION NULL,blpflt_comp_u11 DOUBLE PRECISION NULL,bscflt_comp_u11 DOUBLE PRECISION NULL,bscflt_vent_u11 DOUBLE PRECISION NULL,blpflt_comp_u12 DOUBLE PRECISION NULL,bscflt_comp_u12 DOUBLE PRECISION NULL,bscflt_vent_u12 DOUBLE PRECISION NULL,bflt_fad_u11 DOUBLE PRECISION NULL,bflt_fad_u12 DOUBLE PRECISION NULL,bflt_rad_u11 DOUBLE PRECISION NULL,bflt_rad_u12 DOUBLE PRECISION NULL,bflt_ap_u11 DOUBLE PRECISION NULL,bflt_expboard_u1 DOUBLE PRECISION NULL,bflt_frstemp_u1 DOUBLE PRECISION NULL,bflt_rnttemp_u1 DOUBLE PRECISION NULL,bflt_splytemp_u11 DOUBLE PRECISION NULL,bflt_splytemp_u12 DOUBLE PRECISION NULL,bflt_coiltemp_u11 DOUBLE PRECISION NULL,bflt_coiltemp_u12 DOUBLE PRECISION NULL,bflt_insptemp_u11 DOUBLE PRECISION NULL,bflt_insptemp_u12 DOUBLE PRECISION NULL,bflt_lowpres_u11 DOUBLE PRECISION NULL,bflt_lowpres_u12 DOUBLE PRECISION NULL,bflt_highpres_u11 DOUBLE PRECISION NULL,bflt_highpres_u12 DOUBLE PRECISION NULL,bflt_diffpres_u1 DOUBLE PRECISION NULL,bocflt_ef_u21 DOUBLE PRECISION NULL,bocflt_ef_u22 DOUBLE PRECISION NULL,bocflt_cf_u21 DOUBLE PRECISION NULL,bocflt_cf_u22 DOUBLE PRECISION NULL,bflt_vfd_u21 DOUBLE PRECISION NULL,bflt_vfd_com_u21 DOUBLE PRECISION NULL,bflt_vfd_u22 DOUBLE PRECISION NULL,bflt_vfd_com_u22 DOUBLE PRECISION NULL,blpflt_comp_u21 DOUBLE PRECISION NULL,bscflt_comp_u21 DOUBLE PRECISION NULL,bscflt_vent_u21 DOUBLE PRECISION NULL,blpflt_comp_u22 DOUBLE PRECISION NULL,bscflt_comp_u22 DOUBLE PRECISION NULL,bscflt_vent_u22 DOUBLE PRECISION NULL,bflt_fad_u21 DOUBLE PRECISION NULL,bflt_fad_u22 DOUBLE PRECISION NULL,bflt_rad_u21 DOUBLE PRECISION NULL,bflt_rad_u22 DOUBLE PRECISION NULL,bflt_ap_u21 DOUBLE PRECISION NULL,bflt_expboard_u2 DOUBLE PRECISION NULL,bflt_frstemp_u2 DOUBLE PRECISION NULL,bflt_rnttemp_u2 DOUBLE PRECISION NULL,bflt_splytemp_u21 DOUBLE PRECISION NULL,bflt_splytemp_u22 DOUBLE PRECISION NULL,bflt_coiltemp_u21 DOUBLE PRECISION NULL,bflt_coiltemp_u22 DOUBLE PRECISION NULL,bflt_insptemp_u21 DOUBLE PRECISION NULL,bflt_insptemp_u22 DOUBLE PRECISION NULL,bflt_lowpres_u21 DOUBLE PRECISION NULL,bflt_lowpres_u22 DOUBLE PRECISION NULL,bflt_highpres_u21 DOUBLE PRECISION NULL,bflt_highpres_u22 DOUBLE PRECISION NULL,bflt_diffpres_u2 DOUBLE PRECISION NULL,bflt_emergivt DOUBLE PRECISION NULL,bflt_vehtemp_u1 DOUBLE PRECISION NULL,bflt_vehtemp_u2 DOUBLE PRECISION NULL,bflt_airmon_u1 DOUBLE PRECISION NULL,bflt_airmon_u2 DOUBLE PRECISION NULL,bflt_currentmon DOUBLE PRECISION NULL,bflt_tcms DOUBLE PRECISION NULL,bflt_tempover DOUBLE PRECISION NULL,bflt_powersupply_u1 DOUBLE PRECISION NULL,bflt_powersupply_u2 DOUBLE PRECISION NULL,bflt_exhaustfan DOUBLE PRECISION NULL,bflt_exhaustval DOUBLE PRECISION NULL,fas_sys DOUBLE PRECISION NULL,ras_sys DOUBLE PRECISION NULL,tic DOUBLE PRECISION NULL,load DOUBLE PRECISION NULL,tveh_1 DOUBLE PRECISION NULL,tveh_2 DOUBLE PRECISION NULL,aq_t_u1 DOUBLE PRECISION NULL,aq_h_u1 DOUBLE PRECISION NULL,aq_co2_u1 DOUBLE PRECISION NULL,aq_tvoc_u1 DOUBLE PRECISION NULL,aq_formald_u1 DOUBLE PRECISION NULL,aq_pm2_5_u1 DOUBLE PRECISION NULL,aq_pm10_u1 DOUBLE PRECISION NULL,wmode_u1 DOUBLE PRECISION NULL,presdiff_u1 DOUBLE PRECISION NULL,fas_u1 DOUBLE PRECISION NULL,ras_u1 DOUBLE PRECISION NULL,fadpos_u1 DOUBLE PRECISION NULL,radpos_u1 DOUBLE PRECISION NULL,f_cp_u11 DOUBLE PRECISION NULL,i_cp_u11 DOUBLE PRECISION NULL,v_cp_u11 DOUBLE PRECISION NULL,p_cp_u11 DOUBLE PRECISION NULL,suckt_u11 DOUBLE PRECISION NULL,suckp_u11 DOUBLE PRECISION NULL,sp_u11 DOUBLE PRECISION NULL,eevpos_u11 DOUBLE PRECISION NULL,highpress_u11 DOUBLE PRECISION NULL,sas_u11 DOUBLE PRECISION NULL,ices_u11 DOUBLE PRECISION NULL,f_cp_u12 DOUBLE PRECISION NULL,i_cp_u12 DOUBLE PRECISION NULL,v_cp_u12 DOUBLE PRECISION NULL,p_cp_u12 DOUBLE PRECISION NULL,suckt_u12 DOUBLE PRECISION NULL,suckp_u12 DOUBLE PRECISION NULL,sp_u12 DOUBLE PRECISION NULL,eevpos_u12 DOUBLE PRECISION NULL,highpress_u12 DOUBLE PRECISION NULL,sas_u12 DOUBLE PRECISION NULL,ices_u12 DOUBLE PRECISION NULL,aq_t_u2 DOUBLE PRECISION NULL,aq_h_u2 DOUBLE PRECISION NULL,aq_co2_u2 DOUBLE PRECISION NULL,aq_tvoc_u2 DOUBLE PRECISION NULL,aq_formald_u2 DOUBLE PRECISION NULL,aq_pm2_5_u2 DOUBLE PRECISION NULL,aq_pm10_u2 DOUBLE PRECISION NULL,wmode_u2 DOUBLE PRECISION NULL,presdiff_u2 DOUBLE PRECISION NULL,fas_u2 DOUBLE PRECISION NULL,ras_u2 DOUBLE PRECISION NULL,fadpos_u2 DOUBLE PRECISION NULL,radpos_u2 DOUBLE PRECISION NULL,f_cp_u21 DOUBLE PRECISION NULL,i_cp_u21 DOUBLE PRECISION NULL,v_cp_u21 DOUBLE PRECISION NULL,p_cp_u21 DOUBLE PRECISION NULL,suckt_u21 DOUBLE PRECISION NULL,suckp_u21 DOUBLE PRECISION NULL,sp_u21 DOUBLE PRECISION NULL,eevpos_u21 DOUBLE PRECISION NULL,highpress_u21 DOUBLE PRECISION NULL,sas_u21 DOUBLE PRECISION NULL,ices_u21 DOUBLE PRECISION NULL,f_cp_u22 DOUBLE PRECISION NULL,i_cp_u22 DOUBLE PRECISION NULL,v_cp_u22 DOUBLE PRECISION NULL,p_cp_u22 DOUBLE PRECISION NULL,suckt_u22 DOUBLE PRECISION NULL,suckp_u22 DOUBLE PRECISION NULL,sp_u22 DOUBLE PRECISION NULL,eevpos_u22 DOUBLE PRECISION NULL,highpress_u22 DOUBLE PRECISION NULL,sas_u22 DOUBLE PRECISION NULL,ices_u22 DOUBLE PRECISION NULL,i_ef_u11 DOUBLE PRECISION NULL,i_ef_u12 DOUBLE PRECISION NULL,i_cf_u11 DOUBLE PRECISION NULL,i_cf_u12 DOUBLE PRECISION NULL,i_ef_u21 DOUBLE PRECISION NULL,i_ef_u22 DOUBLE PRECISION NULL,i_cf_u21 DOUBLE PRECISION NULL,i_cf_u22 DOUBLE PRECISION NULL,i_hvac_u1 DOUBLE PRECISION NULL,i_hvac_u2 DOUBLE PRECISION NULL,i_exufan DOUBLE PRECISION NULL,dwpower DOUBLE PRECISION NULL,dwemerg_op_tm DOUBLE PRECISION NULL,dwemerg_op_cnt DOUBLE PRECISION NULL,dwef_op_tm_u11 DOUBLE PRECISION NULL,dwcf_op_tm_u11 DOUBLE PRECISION NULL,dwcp_op_tm_u11 DOUBLE PRECISION NULL,dwcp_op_tm_u12 DOUBLE PRECISION NULL,dwfad_op_cnt_u1 DOUBLE PRECISION NULL,dwrad_op_cnt_u1 DOUBLE PRECISION NULL,dwef_op_cnt_u11 DOUBLE PRECISION NULL,dwcf_op_cnt_u11 DOUBLE PRECISION NULL,dwcp_op_cnt_u11 DOUBLE PRECISION NULL,dwcp_op_cnt_u12 DOUBLE PRECISION NULL,dwef_op_tm_u21 DOUBLE PRECISION NULL,dwcf_op_tm_u21 DOUBLE PRECISION NULL,dwcp_op_tm_u21 DOUBLE PRECISION NULL,dwcp_op_tm_u22 DOUBLE PRECISION NULL,dwfad_op_cnt_u2 DOUBLE PRECISION NULL,dwrad_op_cnt_u2 DOUBLE PRECISION NULL,dwef_op_cnt_u21 DOUBLE PRECISION NULL,dwcf_op_cnt_u21 DOUBLE PRECISION NULL,dwcp_op_cnt_u21 DOUBLE PRECISION NULL,dwcp_op_cnt_u22 DOUBLE PRECISION NULL,dwexufan_op_tm DOUBLE PRECISION NULL,dwexufan_op_cnt DOUBLE PRECISION NULL,dwdmpexu_op_cnt DOUBLE PRECISION NULL);"
            create_pro_table = """
            CREATE TABLE IF NOT EXISTS pro_macda (
                msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                msg_calc_parse_time TEXT NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_flag DOUBLE PRECISION NULL,
                dvc_train_no DOUBLE PRECISION NULL,
                dvc_carriage_no DOUBLE PRECISION NULL,
                dvc_year DOUBLE PRECISION NULL,
                dvc_month DOUBLE PRECISION NULL,
                dvc_day DOUBLE PRECISION NULL,
                dvc_hour DOUBLE PRECISION NULL,
                dvc_minute DOUBLE PRECISION NULL,
                dvc_second DOUBLE PRECISION NULL,
                cfbk_ef_u11 DOUBLE PRECISION NULL,
                cfbk_cf_u11 DOUBLE PRECISION NULL,
                cfbk_cf_u12 DOUBLE PRECISION NULL,
                cfbk_comp_u11 DOUBLE PRECISION NULL,
                cfbk_comp_u12 DOUBLE PRECISION NULL,
                cfbk_ap_u11 DOUBLE PRECISION NULL,
                cfbk_ap_u12 DOUBLE PRECISION NULL,
                cfbk_ef_u21 DOUBLE PRECISION NULL,
                cfbk_cf_u21 DOUBLE PRECISION NULL,
                cfbk_cf_u22 DOUBLE PRECISION NULL,
                cfbk_comp_u21 DOUBLE PRECISION NULL,
                cfbk_comp_u22 DOUBLE PRECISION NULL,
                cfbk_ap_u21 DOUBLE PRECISION NULL,
                cfbk_ap_u22 DOUBLE PRECISION NULL,
                cfbk_tpp_u1 DOUBLE PRECISION NULL,
                cfbk_tpp_u2 DOUBLE PRECISION NULL,
                cfbk_ev_u1 DOUBLE PRECISION NULL,
                cfbk_ev_u2 DOUBLE PRECISION NULL,
                cfbk_ewd DOUBLE PRECISION NULL,
                cfbk_exufan DOUBLE PRECISION NULL,
                bocflt_ef_u11 DOUBLE PRECISION NULL,
                bocflt_ef_u12 DOUBLE PRECISION NULL,
                bocflt_cf_u11 DOUBLE PRECISION NULL,
                bocflt_cf_u12 DOUBLE PRECISION NULL,
                bflt_vfd_u11 DOUBLE PRECISION NULL,
                bflt_vfd_com_u11 DOUBLE PRECISION NULL,
                bflt_vfd_u12 DOUBLE PRECISION NULL,
                bflt_vfd_com_u12 DOUBLE PRECISION NULL,
                blpflt_comp_u11 DOUBLE PRECISION NULL,
                bscflt_comp_u11 DOUBLE PRECISION NULL,
                bscflt_vent_u11 DOUBLE PRECISION NULL,
                blpflt_comp_u12 DOUBLE PRECISION NULL,
                bscflt_comp_u12 DOUBLE PRECISION NULL,
                bscflt_vent_u12 DOUBLE PRECISION NULL,
                bflt_fad_u11 DOUBLE PRECISION NULL,
                bflt_fad_u12 DOUBLE PRECISION NULL,
                bflt_fad_u13 DOUBLE PRECISION NULL,
                bflt_fad_u14 DOUBLE PRECISION NULL,
                bflt_rad_u11 DOUBLE PRECISION NULL,
                bflt_rad_u12 DOUBLE PRECISION NULL,
                bflt_rad_u13 DOUBLE PRECISION NULL,
                bflt_rad_u14 DOUBLE PRECISION NULL,
                bflt_ap_u11 DOUBLE PRECISION NULL,
                bflt_ap_u12 DOUBLE PRECISION NULL,
                bflt_expboard_u1 DOUBLE PRECISION NULL,
                bflt_frstemp_u1 DOUBLE PRECISION NULL,
                bflt_rnttemp_u1 DOUBLE PRECISION NULL,
                bflt_splytemp_u11 DOUBLE PRECISION NULL,
                bflt_splytemp_u12 DOUBLE PRECISION NULL,
                bflt_insptemp_u11 DOUBLE PRECISION NULL,
                bflt_insptemp_u12 DOUBLE PRECISION NULL,
                bflt_lowpres_u11 DOUBLE PRECISION NULL,
                bflt_lowpres_u12 DOUBLE PRECISION NULL,
                bflt_highpres_u11 DOUBLE PRECISION NULL,
                bflt_highpres_u12 DOUBLE PRECISION NULL,
                bflt_diffpres_u1 DOUBLE PRECISION NULL,
                bocflt_ef_u21 DOUBLE PRECISION NULL,
                bocflt_ef_u22 DOUBLE PRECISION NULL,
                bocflt_cf_u21 DOUBLE PRECISION NULL,
                bocflt_cf_u22 DOUBLE PRECISION NULL,
                bflt_vfd_u21 DOUBLE PRECISION NULL,
                bflt_vfd_com_u21 DOUBLE PRECISION NULL,
                bflt_vfd_u22 DOUBLE PRECISION NULL,
                bflt_vfd_com_u22 DOUBLE PRECISION NULL,
                blpflt_comp_u21 DOUBLE PRECISION NULL,
                bscflt_comp_u21 DOUBLE PRECISION NULL,
                bscflt_vent_u21 DOUBLE PRECISION NULL,
                blpflt_comp_u22 DOUBLE PRECISION NULL,
                bscflt_comp_u22 DOUBLE PRECISION NULL,
                bscflt_vent_u22 DOUBLE PRECISION NULL,
                bflt_fad_u21 DOUBLE PRECISION NULL,
                bflt_fad_u22 DOUBLE PRECISION NULL,
                bflt_fad_u23 DOUBLE PRECISION NULL,
                bflt_fad_u24 DOUBLE PRECISION NULL,
                bflt_rad_u21 DOUBLE PRECISION NULL,
                bflt_rad_u22 DOUBLE PRECISION NULL,
                bflt_rad_u23 DOUBLE PRECISION NULL,
                bflt_rad_u24 DOUBLE PRECISION NULL,
                bflt_ap_u21 DOUBLE PRECISION NULL,
                bflt_ap_u22 DOUBLE PRECISION NULL,
                bflt_expboard_u2 DOUBLE PRECISION NULL,
                bflt_frstemp_u2 DOUBLE PRECISION NULL,
                bflt_rnttemp_u2 DOUBLE PRECISION NULL,
                bflt_splytemp_u21 DOUBLE PRECISION NULL,
                bflt_splytemp_u22 DOUBLE PRECISION NULL,
                bflt_insptemp_u21 DOUBLE PRECISION NULL,
                bflt_insptemp_u22 DOUBLE PRECISION NULL,
                bflt_lowpres_u21 DOUBLE PRECISION NULL,
                bflt_lowpres_u22 DOUBLE PRECISION NULL,
                bflt_highpres_u21 DOUBLE PRECISION NULL,
                bflt_highpres_u22 DOUBLE PRECISION NULL,
                bflt_diffpres_u2 DOUBLE PRECISION NULL,
                bflt_emergivt DOUBLE PRECISION NULL,
                bflt_vehtemp_u1 DOUBLE PRECISION NULL,
                bflt_vehhum_u1 DOUBLE PRECISION NULL,
                bflt_vehtemp_u2 DOUBLE PRECISION NULL,
                bflt_vehhum_u2 DOUBLE PRECISION NULL,
                bflt_airmon_u1 DOUBLE PRECISION NULL,
                bflt_airmon_u2 DOUBLE PRECISION NULL,
                bflt_currentmon DOUBLE PRECISION NULL,
                bflt_tcms DOUBLE PRECISION NULL,
                bscflt_ef_u1 DOUBLE PRECISION NULL,
                bscflt_cf_u1 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u11 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u12 DOUBLE PRECISION NULL,
                bscflt_ef_u2 DOUBLE PRECISION NULL,
                bscflt_cf_u2 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u21 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u22 DOUBLE PRECISION NULL,
                bflt_ef_cnt_u1 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u11 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u12 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u11 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u12 DOUBLE PRECISION NULL,
                bflt__ev_cnt_u1 DOUBLE PRECISION NULL,
                bflt_ef_cnt_u2 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u21 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u22 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u21 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u22 DOUBLE PRECISION NULL,
                bflt__ev_cnt_u2 DOUBLE PRECISION NULL,
                bflt_tempover DOUBLE PRECISION NULL,
                fas_sys DOUBLE PRECISION NULL,
                ras_sys DOUBLE PRECISION NULL,
                tic DOUBLE PRECISION NULL,
                load DOUBLE PRECISION NULL,
                tveh_1 DOUBLE PRECISION NULL,
                humdity_1 DOUBLE PRECISION NULL,
                tveh_2 DOUBLE PRECISION NULL,
                humdity_2 DOUBLE PRECISION NULL,
                aq_t_u1 DOUBLE PRECISION NULL,
                aq_h_u1 DOUBLE PRECISION NULL,
                aq_co2_u1 DOUBLE PRECISION NULL,
                aq_tvoc_u1 DOUBLE PRECISION NULL,
                aq_pm2_5_u1 DOUBLE PRECISION NULL,
                aq_pm10_u1 DOUBLE PRECISION NULL,
                wmode_u1 DOUBLE PRECISION NULL,
                presdiff_u1 DOUBLE PRECISION NULL,
                fas_u1 DOUBLE PRECISION NULL,
                ras_u1 DOUBLE PRECISION NULL,
                fadpos_u1 DOUBLE PRECISION NULL,
                radpos_u1 DOUBLE PRECISION NULL,
                f_cp_u11 DOUBLE PRECISION NULL,
                i_cp_u11 DOUBLE PRECISION NULL,
                v_cp_u11 DOUBLE PRECISION NULL,
                p_cp_u11 DOUBLE PRECISION NULL,
                suckt_u11 DOUBLE PRECISION NULL,
                suckp_u11 DOUBLE PRECISION NULL,
                sp_u11 DOUBLE PRECISION NULL,
                eevpos_u11 DOUBLE PRECISION NULL,
                highpress_u11 DOUBLE PRECISION NULL,
                sas_u11 DOUBLE PRECISION NULL,
                f_cp_u12 DOUBLE PRECISION NULL,
                i_cp_u12 DOUBLE PRECISION NULL,
                v_cp_u12 DOUBLE PRECISION NULL,
                p_cp_u12 DOUBLE PRECISION NULL,
                suckt_u12 DOUBLE PRECISION NULL,
                suckp_u12 DOUBLE PRECISION NULL,
                sp_u12 DOUBLE PRECISION NULL,
                eevpos_u12 DOUBLE PRECISION NULL,
                highpress_u12 DOUBLE PRECISION NULL,
                sas_u12 DOUBLE PRECISION NULL,
                aq_t_u2 DOUBLE PRECISION NULL,
                aq_h_u2 DOUBLE PRECISION NULL,
                aq_co2_u2 DOUBLE PRECISION NULL,
                aq_tvoc_u2 DOUBLE PRECISION NULL,
                aq_pm2_5_u2 DOUBLE PRECISION NULL,
                aq_pm10_u2 DOUBLE PRECISION NULL,
                wmode_u2 DOUBLE PRECISION NULL,
                presdiff_u2 DOUBLE PRECISION NULL,
                fas_u2 DOUBLE PRECISION NULL,
                ras_u2 DOUBLE PRECISION NULL,
                fadpos_u2 DOUBLE PRECISION NULL,
                radpos_u2 DOUBLE PRECISION NULL,
                f_cp_u21 DOUBLE PRECISION NULL,
                i_cp_u21 DOUBLE PRECISION NULL,
                v_cp_u21 DOUBLE PRECISION NULL,
                p_cp_u21 DOUBLE PRECISION NULL,
                suckt_u21 DOUBLE PRECISION NULL,
                suckp_u21 DOUBLE PRECISION NULL,
                sp_u21 DOUBLE PRECISION NULL,
                eevpos_u21 DOUBLE PRECISION NULL,
                highpress_u21 DOUBLE PRECISION NULL,
                sas_u21 DOUBLE PRECISION NULL,
                f_cp_u22 DOUBLE PRECISION NULL,
                i_cp_u22 DOUBLE PRECISION NULL,
                v_cp_u22 DOUBLE PRECISION NULL,
                p_cp_u22 DOUBLE PRECISION NULL,
                suckt_u22 DOUBLE PRECISION NULL,
                suckp_u22 DOUBLE PRECISION NULL,
                sp_u22 DOUBLE PRECISION NULL,
                eevpos_u22 DOUBLE PRECISION NULL,
                highpress_u22 DOUBLE PRECISION NULL,
                sas_u22 DOUBLE PRECISION NULL,
                i_ef_u11 DOUBLE PRECISION NULL,
                i_ef_u12 DOUBLE PRECISION NULL,
                i_cf_u11 DOUBLE PRECISION NULL,
                i_cf_u12 DOUBLE PRECISION NULL,
                i_ef_u21 DOUBLE PRECISION NULL,
                i_ef_u22 DOUBLE PRECISION NULL,
                i_cf_u21 DOUBLE PRECISION NULL,
                i_cf_u22 DOUBLE PRECISION NULL,
                i_hvac_u1 DOUBLE PRECISION NULL,
                i_hvac_u2 DOUBLE PRECISION NULL,
                dwpower DOUBLE PRECISION NULL,
                dwemerg_op_tm DOUBLE PRECISION NULL,
                dwemerg_op_cnt DOUBLE PRECISION NULL,
                dwef_op_tm_u11 DOUBLE PRECISION NULL,
                dwef_op_tm_u12 DOUBLE PRECISION NULL,
                dwcf_op_tm_u11 DOUBLE PRECISION NULL,
                dwcf_op_tm_u12 DOUBLE PRECISION NULL,
                dwcp_op_tm_u11 DOUBLE PRECISION NULL,
                dwcp_op_tm_u12 DOUBLE PRECISION NULL,
                dwap_op_tm_u11 DOUBLE PRECISION NULL,
                dwap_op_tm_u12 DOUBLE PRECISION NULL,
                dwfad_op_cnt_u1 DOUBLE PRECISION NULL,
                dwrad_op_cnt_u1 DOUBLE PRECISION NULL,
                dwef_op_cnt_u11 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u11 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u12 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u11 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u12 DOUBLE PRECISION NULL,
                dwap_op_cnt_u11 DOUBLE PRECISION NULL,
                dwap_op_cnt_u12 DOUBLE PRECISION NULL,
                dwef_op_tm_u21 DOUBLE PRECISION NULL,
                dwcf_op_tm_u21 DOUBLE PRECISION NULL,
                dwcf_op_tm_u22 DOUBLE PRECISION NULL,
                dwcp_op_tm_u21 DOUBLE PRECISION NULL,
                dwcp_op_tm_u22 DOUBLE PRECISION NULL,
                dwap_op_tm_u21 DOUBLE PRECISION NULL,
                dwap_op_tm_u22 DOUBLE PRECISION NULL,
                dwfad_op_cnt_u2 DOUBLE PRECISION NULL,
                dwrad_op_cnt_u2 DOUBLE PRECISION NULL,
                dwef_op_cnt_u21 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u21 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u22 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u21 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u22 DOUBLE PRECISION NULL,
                dwap_op_cnt_u21 DOUBLE PRECISION NULL,
                dwap_op_cnt_u22 DOUBLE PRECISION NULL
            );
            """
            create_dev_table = """
            CREATE TABLE IF NOT EXISTS dev_macda (
                msg_calc_dvc_time TEXT NOT NULL,
                msg_calc_parse_time TIMESTAMPTZ NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_flag DOUBLE PRECISION NULL,
                dvc_train_no DOUBLE PRECISION NULL,
                dvc_carriage_no DOUBLE PRECISION NULL,
                dvc_year DOUBLE PRECISION NULL,
                dvc_month DOUBLE PRECISION NULL,
                dvc_day DOUBLE PRECISION NULL,
                dvc_hour DOUBLE PRECISION NULL,
                dvc_minute DOUBLE PRECISION NULL,
                dvc_second DOUBLE PRECISION NULL,
                cfbk_ef_u11 DOUBLE PRECISION NULL,
                cfbk_cf_u11 DOUBLE PRECISION NULL,
                cfbk_cf_u12 DOUBLE PRECISION NULL,
                cfbk_comp_u11 DOUBLE PRECISION NULL,
                cfbk_comp_u12 DOUBLE PRECISION NULL,
                cfbk_ap_u11 DOUBLE PRECISION NULL,
                cfbk_ap_u12 DOUBLE PRECISION NULL,
                cfbk_ef_u21 DOUBLE PRECISION NULL,
                cfbk_cf_u21 DOUBLE PRECISION NULL,
                cfbk_cf_u22 DOUBLE PRECISION NULL,
                cfbk_comp_u21 DOUBLE PRECISION NULL,
                cfbk_comp_u22 DOUBLE PRECISION NULL,
                cfbk_ap_u21 DOUBLE PRECISION NULL,
                cfbk_ap_u22 DOUBLE PRECISION NULL,
                cfbk_tpp_u1 DOUBLE PRECISION NULL,
                cfbk_tpp_u2 DOUBLE PRECISION NULL,
                cfbk_ev_u1 DOUBLE PRECISION NULL,
                cfbk_ev_u2 DOUBLE PRECISION NULL,
                cfbk_ewd DOUBLE PRECISION NULL,
                cfbk_exufan DOUBLE PRECISION NULL,
                bocflt_ef_u11 DOUBLE PRECISION NULL,
                bocflt_ef_u12 DOUBLE PRECISION NULL,
                bocflt_cf_u11 DOUBLE PRECISION NULL,
                bocflt_cf_u12 DOUBLE PRECISION NULL,
                bflt_vfd_u11 DOUBLE PRECISION NULL,
                bflt_vfd_com_u11 DOUBLE PRECISION NULL,
                bflt_vfd_u12 DOUBLE PRECISION NULL,
                bflt_vfd_com_u12 DOUBLE PRECISION NULL,
                blpflt_comp_u11 DOUBLE PRECISION NULL,
                bscflt_comp_u11 DOUBLE PRECISION NULL,
                bscflt_vent_u11 DOUBLE PRECISION NULL,
                blpflt_comp_u12 DOUBLE PRECISION NULL,
                bscflt_comp_u12 DOUBLE PRECISION NULL,
                bscflt_vent_u12 DOUBLE PRECISION NULL,
                bflt_fad_u11 DOUBLE PRECISION NULL,
                bflt_fad_u12 DOUBLE PRECISION NULL,
                bflt_fad_u13 DOUBLE PRECISION NULL,
                bflt_fad_u14 DOUBLE PRECISION NULL,
                bflt_rad_u11 DOUBLE PRECISION NULL,
                bflt_rad_u12 DOUBLE PRECISION NULL,
                bflt_rad_u13 DOUBLE PRECISION NULL,
                bflt_rad_u14 DOUBLE PRECISION NULL,
                bflt_ap_u11 DOUBLE PRECISION NULL,
                bflt_ap_u12 DOUBLE PRECISION NULL,
                bflt_expboard_u1 DOUBLE PRECISION NULL,
                bflt_frstemp_u1 DOUBLE PRECISION NULL,
                bflt_rnttemp_u1 DOUBLE PRECISION NULL,
                bflt_splytemp_u11 DOUBLE PRECISION NULL,
                bflt_splytemp_u12 DOUBLE PRECISION NULL,
                bflt_insptemp_u11 DOUBLE PRECISION NULL,
                bflt_insptemp_u12 DOUBLE PRECISION NULL,
                bflt_lowpres_u11 DOUBLE PRECISION NULL,
                bflt_lowpres_u12 DOUBLE PRECISION NULL,
                bflt_highpres_u11 DOUBLE PRECISION NULL,
                bflt_highpres_u12 DOUBLE PRECISION NULL,
                bflt_diffpres_u1 DOUBLE PRECISION NULL,
                bocflt_ef_u21 DOUBLE PRECISION NULL,
                bocflt_ef_u22 DOUBLE PRECISION NULL,
                bocflt_cf_u21 DOUBLE PRECISION NULL,
                bocflt_cf_u22 DOUBLE PRECISION NULL,
                bflt_vfd_u21 DOUBLE PRECISION NULL,
                bflt_vfd_com_u21 DOUBLE PRECISION NULL,
                bflt_vfd_u22 DOUBLE PRECISION NULL,
                bflt_vfd_com_u22 DOUBLE PRECISION NULL,
                blpflt_comp_u21 DOUBLE PRECISION NULL,
                bscflt_comp_u21 DOUBLE PRECISION NULL,
                bscflt_vent_u21 DOUBLE PRECISION NULL,
                blpflt_comp_u22 DOUBLE PRECISION NULL,
                bscflt_comp_u22 DOUBLE PRECISION NULL,
                bscflt_vent_u22 DOUBLE PRECISION NULL,
                bflt_fad_u21 DOUBLE PRECISION NULL,
                bflt_fad_u22 DOUBLE PRECISION NULL,
                bflt_fad_u23 DOUBLE PRECISION NULL,
                bflt_fad_u24 DOUBLE PRECISION NULL,
                bflt_rad_u21 DOUBLE PRECISION NULL,
                bflt_rad_u22 DOUBLE PRECISION NULL,
                bflt_rad_u23 DOUBLE PRECISION NULL,
                bflt_rad_u24 DOUBLE PRECISION NULL,
                bflt_ap_u21 DOUBLE PRECISION NULL,
                bflt_ap_u22 DOUBLE PRECISION NULL,
                bflt_expboard_u2 DOUBLE PRECISION NULL,
                bflt_frstemp_u2 DOUBLE PRECISION NULL,
                bflt_rnttemp_u2 DOUBLE PRECISION NULL,
                bflt_splytemp_u21 DOUBLE PRECISION NULL,
                bflt_splytemp_u22 DOUBLE PRECISION NULL,
                bflt_insptemp_u21 DOUBLE PRECISION NULL,
                bflt_insptemp_u22 DOUBLE PRECISION NULL,
                bflt_lowpres_u21 DOUBLE PRECISION NULL,
                bflt_lowpres_u22 DOUBLE PRECISION NULL,
                bflt_highpres_u21 DOUBLE PRECISION NULL,
                bflt_highpres_u22 DOUBLE PRECISION NULL,
                bflt_diffpres_u2 DOUBLE PRECISION NULL,
                bflt_emergivt DOUBLE PRECISION NULL,
                bflt_vehtemp_u1 DOUBLE PRECISION NULL,
                bflt_vehhum_u1 DOUBLE PRECISION NULL,
                bflt_vehtemp_u2 DOUBLE PRECISION NULL,
                bflt_vehhum_u2 DOUBLE PRECISION NULL,
                bflt_airmon_u1 DOUBLE PRECISION NULL,
                bflt_airmon_u2 DOUBLE PRECISION NULL,
                bflt_currentmon DOUBLE PRECISION NULL,
                bflt_tcms DOUBLE PRECISION NULL,
                bscflt_ef_u1 DOUBLE PRECISION NULL,
                bscflt_cf_u1 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u11 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u12 DOUBLE PRECISION NULL,
                bscflt_ef_u2 DOUBLE PRECISION NULL,
                bscflt_cf_u2 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u21 DOUBLE PRECISION NULL,
                bscflt_vfd_pw_u22 DOUBLE PRECISION NULL,
                bflt_ef_cnt_u1 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u11 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u12 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u11 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u12 DOUBLE PRECISION NULL,
                bflt__ev_cnt_u1 DOUBLE PRECISION NULL,
                bflt_ef_cnt_u2 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u21 DOUBLE PRECISION NULL,
                bflt_cf_cnt_u22 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u21 DOUBLE PRECISION NULL,
                bflt_vfd_cnt_u22 DOUBLE PRECISION NULL,
                bflt__ev_cnt_u2 DOUBLE PRECISION NULL,
                bflt_tempover DOUBLE PRECISION NULL,
                fas_sys DOUBLE PRECISION NULL,
                ras_sys DOUBLE PRECISION NULL,
                tic DOUBLE PRECISION NULL,
                load DOUBLE PRECISION NULL,
                tveh_1 DOUBLE PRECISION NULL,
                humdity_1 DOUBLE PRECISION NULL,
                tveh_2 DOUBLE PRECISION NULL,
                humdity_2 DOUBLE PRECISION NULL,
                aq_t_u1 DOUBLE PRECISION NULL,
                aq_h_u1 DOUBLE PRECISION NULL,
                aq_co2_u1 DOUBLE PRECISION NULL,
                aq_tvoc_u1 DOUBLE PRECISION NULL,
                aq_pm2_5_u1 DOUBLE PRECISION NULL,
                aq_pm10_u1 DOUBLE PRECISION NULL,
                wmode_u1 DOUBLE PRECISION NULL,
                presdiff_u1 DOUBLE PRECISION NULL,
                fas_u1 DOUBLE PRECISION NULL,
                ras_u1 DOUBLE PRECISION NULL,
                fadpos_u1 DOUBLE PRECISION NULL,
                radpos_u1 DOUBLE PRECISION NULL,
                f_cp_u11 DOUBLE PRECISION NULL,
                i_cp_u11 DOUBLE PRECISION NULL,
                v_cp_u11 DOUBLE PRECISION NULL,
                p_cp_u11 DOUBLE PRECISION NULL,
                suckt_u11 DOUBLE PRECISION NULL,
                suckp_u11 DOUBLE PRECISION NULL,
                sp_u11 DOUBLE PRECISION NULL,
                eevpos_u11 DOUBLE PRECISION NULL,
                highpress_u11 DOUBLE PRECISION NULL,
                sas_u11 DOUBLE PRECISION NULL,
                f_cp_u12 DOUBLE PRECISION NULL,
                i_cp_u12 DOUBLE PRECISION NULL,
                v_cp_u12 DOUBLE PRECISION NULL,
                p_cp_u12 DOUBLE PRECISION NULL,
                suckt_u12 DOUBLE PRECISION NULL,
                suckp_u12 DOUBLE PRECISION NULL,
                sp_u12 DOUBLE PRECISION NULL,
                eevpos_u12 DOUBLE PRECISION NULL,
                highpress_u12 DOUBLE PRECISION NULL,
                sas_u12 DOUBLE PRECISION NULL,
                aq_t_u2 DOUBLE PRECISION NULL,
                aq_h_u2 DOUBLE PRECISION NULL,
                aq_co2_u2 DOUBLE PRECISION NULL,
                aq_tvoc_u2 DOUBLE PRECISION NULL,
                aq_pm2_5_u2 DOUBLE PRECISION NULL,
                aq_pm10_u2 DOUBLE PRECISION NULL,
                wmode_u2 DOUBLE PRECISION NULL,
                presdiff_u2 DOUBLE PRECISION NULL,
                fas_u2 DOUBLE PRECISION NULL,
                ras_u2 DOUBLE PRECISION NULL,
                fadpos_u2 DOUBLE PRECISION NULL,
                radpos_u2 DOUBLE PRECISION NULL,
                f_cp_u21 DOUBLE PRECISION NULL,
                i_cp_u21 DOUBLE PRECISION NULL,
                v_cp_u21 DOUBLE PRECISION NULL,
                p_cp_u21 DOUBLE PRECISION NULL,
                suckt_u21 DOUBLE PRECISION NULL,
                suckp_u21 DOUBLE PRECISION NULL,
                sp_u21 DOUBLE PRECISION NULL,
                eevpos_u21 DOUBLE PRECISION NULL,
                highpress_u21 DOUBLE PRECISION NULL,
                sas_u21 DOUBLE PRECISION NULL,
                f_cp_u22 DOUBLE PRECISION NULL,
                i_cp_u22 DOUBLE PRECISION NULL,
                v_cp_u22 DOUBLE PRECISION NULL,
                p_cp_u22 DOUBLE PRECISION NULL,
                suckt_u22 DOUBLE PRECISION NULL,
                suckp_u22 DOUBLE PRECISION NULL,
                sp_u22 DOUBLE PRECISION NULL,
                eevpos_u22 DOUBLE PRECISION NULL,
                highpress_u22 DOUBLE PRECISION NULL,
                sas_u22 DOUBLE PRECISION NULL,
                i_ef_u11 DOUBLE PRECISION NULL,
                i_ef_u12 DOUBLE PRECISION NULL,
                i_cf_u11 DOUBLE PRECISION NULL,
                i_cf_u12 DOUBLE PRECISION NULL,
                i_ef_u21 DOUBLE PRECISION NULL,
                i_ef_u22 DOUBLE PRECISION NULL,
                i_cf_u21 DOUBLE PRECISION NULL,
                i_cf_u22 DOUBLE PRECISION NULL,
                i_hvac_u1 DOUBLE PRECISION NULL,
                i_hvac_u2 DOUBLE PRECISION NULL,
                dwpower DOUBLE PRECISION NULL,
                dwemerg_op_tm DOUBLE PRECISION NULL,
                dwemerg_op_cnt DOUBLE PRECISION NULL,
                dwef_op_tm_u11 DOUBLE PRECISION NULL,
                dwef_op_tm_u12 DOUBLE PRECISION NULL,
                dwcf_op_tm_u11 DOUBLE PRECISION NULL,
                dwcf_op_tm_u12 DOUBLE PRECISION NULL,
                dwcp_op_tm_u11 DOUBLE PRECISION NULL,
                dwcp_op_tm_u12 DOUBLE PRECISION NULL,
                dwap_op_tm_u11 DOUBLE PRECISION NULL,
                dwap_op_tm_u12 DOUBLE PRECISION NULL,
                dwfad_op_cnt_u1 DOUBLE PRECISION NULL,
                dwrad_op_cnt_u1 DOUBLE PRECISION NULL,
                dwef_op_cnt_u11 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u11 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u12 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u11 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u12 DOUBLE PRECISION NULL,
                dwap_op_cnt_u11 DOUBLE PRECISION NULL,
                dwap_op_cnt_u12 DOUBLE PRECISION NULL,
                dwef_op_tm_u21 DOUBLE PRECISION NULL,
                dwcf_op_tm_u21 DOUBLE PRECISION NULL,
                dwcf_op_tm_u22 DOUBLE PRECISION NULL,
                dwcp_op_tm_u21 DOUBLE PRECISION NULL,
                dwcp_op_tm_u22 DOUBLE PRECISION NULL,
                dwap_op_tm_u21 DOUBLE PRECISION NULL,
                dwap_op_tm_u22 DOUBLE PRECISION NULL,
                dwfad_op_cnt_u2 DOUBLE PRECISION NULL,
                dwrad_op_cnt_u2 DOUBLE PRECISION NULL,
                dwef_op_cnt_u21 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u21 DOUBLE PRECISION NULL,
                dwcf_op_cnt_u22 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u21 DOUBLE PRECISION NULL,
                dwcp_op_cnt_u22 DOUBLE PRECISION NULL,
                dwap_op_cnt_u21 DOUBLE PRECISION NULL,
                dwap_op_cnt_u22 DOUBLE PRECISION NULL
            );
            """
            create_pro_json_table = "CREATE TABLE IF NOT EXISTS pro_macda_json (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, Indicators JSON);"
            create_dev_json_table = "CREATE TABLE IF NOT EXISTS dev_macda_json (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, Indicators JSON);"
            create_pro_predict_table = "CREATE TABLE IF NOT EXISTS pro_predict (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, ref_leak_u11 integer NOT NULL DEFAULT 0, ref_leak_u12 integer NOT NULL DEFAULT 0, ref_leak_u21 integer NOT NULL DEFAULT 0, ref_leak_u22 integer NOT NULL DEFAULT 0, f_cp_u1 integer NOT NULL DEFAULT 0, f_cp_u2 integer NOT NULL DEFAULT 0, f_fas integer NOT NULL DEFAULT 0, f_ras integer NOT NULL DEFAULT 0, cabin_overtemp integer NOT NULL DEFAULT 0, f_presdiff_u1 integer NOT NULL DEFAULT 0, f_presdiff_u2 integer NOT NULL DEFAULT 0, f_ef_u11 integer NOT NULL DEFAULT 0, f_ef_u12 integer NOT NULL DEFAULT 0, f_ef_u21 integer NOT NULL DEFAULT 0, f_ef_u22 integer NOT NULL DEFAULT 0, f_cf_u11 integer NOT NULL DEFAULT 0, f_cf_u12 integer NOT NULL DEFAULT 0, f_cf_u21 integer NOT NULL DEFAULT 0, f_cf_u22 integer NOT NULL DEFAULT 0, f_exufan integer NOT NULL DEFAULT 0, f_fas_u11 integer NOT NULL DEFAULT 0, f_fas_u12 integer NOT NULL DEFAULT 0, f_fas_u21 integer NOT NULL DEFAULT 0, f_fas_u22 integer NOT NULL DEFAULT 0, f_aq_u1 integer NOT NULL DEFAULT 0, f_aq_u2 integer NOT NULL DEFAULT 0);"
            create_dev_predict_table = "CREATE TABLE IF NOT EXISTS dev_predict (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, ref_leak_u11 integer NOT NULL DEFAULT 0, ref_leak_u12 integer NOT NULL DEFAULT 0, ref_leak_u21 integer NOT NULL DEFAULT 0, ref_leak_u22 integer NOT NULL DEFAULT 0, f_cp_u1 integer NOT NULL DEFAULT 0, f_cp_u2 integer NOT NULL DEFAULT 0, f_fas integer NOT NULL DEFAULT 0, f_ras integer NOT NULL DEFAULT 0, cabin_overtemp integer NOT NULL DEFAULT 0, f_presdiff_u1 integer NOT NULL DEFAULT 0, f_presdiff_u2 integer NOT NULL DEFAULT 0, f_ef_u11 integer NOT NULL DEFAULT 0, f_ef_u12 integer NOT NULL DEFAULT 0, f_ef_u21 integer NOT NULL DEFAULT 0, f_ef_u22 integer NOT NULL DEFAULT 0, f_cf_u11 integer NOT NULL DEFAULT 0, f_cf_u12 integer NOT NULL DEFAULT 0, f_cf_u21 integer NOT NULL DEFAULT 0, f_cf_u22 integer NOT NULL DEFAULT 0, f_exufan integer NOT NULL DEFAULT 0, f_fas_u11 integer NOT NULL DEFAULT 0, f_fas_u12 integer NOT NULL DEFAULT 0, f_fas_u21 integer NOT NULL DEFAULT 0, f_fas_u22 integer NOT NULL DEFAULT 0, f_aq_u1 integer NOT NULL DEFAULT 0, f_aq_u2 integer NOT NULL DEFAULT 0);"
            cur = conn.cursor()
            cur.execute(create_pro_table)
            cur.execute(create_dev_table)
            cur.execute(create_pro_json_table)
            cur.execute(create_dev_json_table)
            cur.execute(create_pro_predict_table)
            cur.execute(create_dev_predict_table)
            cur.execute("SELECT create_hypertable('pro_macda', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('pro_macda_json', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda_json', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('pro_predict', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_predict', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            #cur.execute("SELECT remove_retention_policy('pro_macda', True);")
            cur.execute("SELECT add_retention_policy('pro_macda', INTERVAL '1 year', if_not_exists => true);")
            #cur.execute("SELECT remove_retention_policy('dev_macda', True);")
            cur.execute("SELECT add_retention_policy('dev_macda', INTERVAL '1 year', if_not_exists => true);")
            #cur.execute("SELECT remove_retention_policy('pro_macda_json', True);")
            cur.execute("SELECT add_retention_policy('pro_macda_json', INTERVAL '1 year', if_not_exists => true);")
            #cur.execute("SELECT remove_retention_policy('dev_macda_json', True);")
            cur.execute("SELECT add_retention_policy('dev_macda_json', INTERVAL '1 year', if_not_exists => true);")
            #cur.execute("SELECT remove_retention_policy('pro_predict', True);")
            cur.execute("SELECT add_retention_policy('pro_predict', INTERVAL '1 year', if_not_exists => true);")
            #cur.execute("SELECT remove_retention_policy('dev_predict', True);")
            cur.execute("SELECT add_retention_policy('dev_predict', INTERVAL '1 year', if_not_exists => true);")
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
            log.debug("Check tsdb table ... Success !")
        except Exception as exp:
            log.error('Exception at tsutil.__init__() %s ' % exp)
            traceback.print_exc()

    def insert(self, tablename, jsonobj):
        keylst = []
        valuelst = []
        masklst = []
        #ignorekeys = ['msg_header_code01', 'msg_header_code02', 'msg_carriage_no', 'msg_protocal_version', 'msg_crc']
        for (key, value) in jsonobj.items():
            #if not key in ignorekeys:
            keylst.append(key)
            valuelst.append(str(value))
            masklst.append('%s')
        keystr = ','.join(keylst)
        maskstr = ','.join(masklst)
        insertsql = f"INSERT INTO {tablename} ({keystr}) VALUES ({maskstr})"
        # log.debug(insertsql)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            cur.execute(insertsql, valuelst)
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.insert() %s ' % exp)
            traceback.print_exc()

    def insertjson(self, tablename, jsonobj):
        valuelst = []
        keystr = 'msg_calc_dvc_time, msg_calc_parse_time, msg_calc_dvc_no, msg_calc_train_no, indicators'
        maskstr = '%s, %s, %s, %s, %s'
        insertsql = f"INSERT INTO {tablename} ({keystr}) VALUES ({maskstr})"
        valuelst.append(str(jsonobj['msg_calc_dvc_time']))
        valuelst.append(str(jsonobj['msg_calc_parse_time']))
        valuelst.append(str(jsonobj['msg_calc_dvc_no']))
        valuelst.append(str(jsonobj['msg_calc_train_no']))
        valuelst.append(json.dumps(jsonobj))
        #log.debug(insertsql)
        #log.debug(valuelst)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            cur.execute(insertsql, valuelst)
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.insertjson() %s ' % exp)
            traceback.print_exc()

    def batchinsert(self, tablename, timefieldname, jsonobjlst):
        #ignorekeys = ['msg_header_code01', 'msg_header_code02', 'msg_carriage_no', 'msg_protocal_version', 'msg_crc']
        jsonobj = jsonobjlst[0]['payload']
        cols = []
        for (key, value) in jsonobj.items():
            #if not key in ignorekeys:
            cols.append(key)
        #log.debug(cols)
        records = []
        for jsonobj in jsonobjlst:
            record = []
            for (key, value) in jsonobj['payload'].items():
                #if not key in ignorekeys:
                if key == timefieldname:
                    record.append(self.parse_time(value))
                else:
                    record.append(value)
            records.append(record)
        #log.debug(records)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            mgr = CopyManager(conn, tablename, cols)
            mgr.copy(records)
            conn.commit()
            log.debug("========== Batch Commited")
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.batchinsert() %s ' % exp)
            traceback.print_exc()

    def batchinsertjson(self, tablename, timefieldname, jsonobjlst):
        jsonobj = jsonobjlst[0]['payload']
        cols = ['msg_calc_dvc_no', 'msg_calc_train_no', 'msg_calc_dvc_time', 'msg_calc_parse_time', 'indicators']
        records = []
        for jsonobj in jsonobjlst:
            record = []
            record.append(jsonobj['payload']['msg_calc_dvc_no'])
            record.append(jsonobj['payload']['msg_calc_train_no'])
            if timefieldname == 'msg_calc_dvc_time':
                record.append(self.parse_time(jsonobj['payload']['msg_calc_dvc_time']))
                record.append(jsonobj['payload']['msg_calc_parse_time'])
            else:
                record.append(jsonobj['payload']['msg_calc_dvc_time'])
                record.append(self.parse_time(jsonobj['payload']['msg_calc_parse_time']))
            record.append(json.dumps(jsonobj['payload']))
            records.append(record)
        #log.debug(records)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            mgr = CopyManager(conn, tablename, cols)
            mgr.copy(records)
            conn.commit()
            log.debug("========== Batch Commited")
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.batchinsertjson() %s ' % exp)
            traceback.print_exc()

    def get_predict_data(self, mode):
        querysql = ''
        if mode == 'dev':
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_parse_time, msg_calc_parse_time) as time, max(ref_leak_u11) as ref_leak_u11, max(ref_leak_u12) as ref_leak_u12, max(ref_leak_u21) as ref_leak_u21, max(ref_leak_u22) as ref_leak_u22, max(f_cp_u1) as f_cp_u1, max(f_cp_u2) as f_cp_u2, max(f_fas) as f_fas, max(f_ras) as f_ras, max(cabin_overtemp) as cabin_overtemp, max(f_presdiff_u1) as f_presdiff_u1, max(f_presdiff_u2) as f_presdiff_u2, max(f_ef_u11) as f_ef_u11, max(f_ef_u12) as f_ef_u12, max(f_ef_u21) as f_ef_u21, max(f_ef_u22) as f_ef_u22, max(f_cf_u11) as f_cf_u11, max(f_cf_u12) as f_cf_u12, max(f_cf_u21) as f_cf_u21, max(f_cf_u22) as f_cf_u22, max(f_exufan) as f_exufan, max(f_fas_u11) as f_fas_u11, max(f_fas_u12) as f_fas_u12, max(f_fas_u21) as f_fas_u21, max(f_fas_u22) as f_fas_u22, max(f_aq_u1) as f_aq_u1, max(f_aq_u2) as f_aq_u2 FROM dev_predict WHERE msg_calc_parse_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
        else:
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_dvc_time, msg_calc_dvc_time) as time, max(ref_leak_u11) as ref_leak_u11, max(ref_leak_u12) as ref_leak_u12, max(ref_leak_u21) as ref_leak_u21, max(ref_leak_u22) as ref_leak_u22, max(f_cp_u1) as f_cp_u1, max(f_cp_u2) as f_cp_u2, max(f_fas) as f_fas, max(f_ras) as f_ras, max(cabin_overtemp) as cabin_overtemp, max(f_presdiff_u1) as f_presdiff_u1, max(f_presdiff_u2) as f_presdiff_u2, max(f_ef_u11) as f_ef_u11, max(f_ef_u12) as f_ef_u12, max(f_ef_u21) as f_ef_u21, max(f_ef_u22) as f_ef_u22, max(f_cf_u11) as f_cf_u11, max(f_cf_u12) as f_cf_u12, max(f_cf_u21) as f_cf_u21, max(f_cf_u22) as f_cf_u22, max(f_exufan) as f_exufan, max(f_fas_u11) as f_fas_u11, max(f_fas_u12) as f_fas_u12, max(f_fas_u21) as f_fas_u21, max(f_fas_u22) as f_fas_u22, max(f_aq_u1) as f_aq_u1, max(f_aq_u2) as f_aq_u2 FROM pro_predict WHERE msg_calc_dvc_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_predict_data() %s ' % exp)
            traceback.print_exc()

    def get_fault_data(self, mode):
        querysql = ''
        if mode == 'dev':
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_parse_time, msg_calc_parse_time) as time, max(bocflt_ef_u11) as dvc_bocflt_ef_u11, max(bocflt_ef_u12) as dvc_bocflt_ef_u12, max(bocflt_cf_u11) as dvc_bocflt_cf_u11, max(bocflt_cf_u12) as dvc_bocflt_cf_u12, max(bflt_vfd_u11) as dvc_bflt_vfd_u11, max(blpflt_comp_u11) as dvc_blpflt_comp_u11, max(bscflt_comp_u11) as dvc_bscflt_comp_u11, max(bflt_vfd_u12) as dvc_bflt_vfd_u12, max(blpflt_comp_u12) as dvc_blpflt_comp_u12, max(bscflt_comp_u12) as dvc_bscflt_comp_u12, max(fadpos_u1) as dvc_bflt_fad_u1, max(radpos_u2) as dvc_bflt_rad_u1, max(bflt_ap_u11) as dvc_bflt_airclean_u1, max(bflt_expboard_u1) as dvc_bflt_expboard_u1, max(bflt_frstemp_u1) as dvc_bflt_frstemp_u1, max(bflt_splytemp_u11) as dvc_bflt_splytemp_u11, max(bflt_splytemp_u12) as dvc_bflt_splytemp_u12, max(bflt_rnttemp_u1) as dvc_bflt_rnttemp_u1, max(bflt_coiltemp_u11) as dvc_bflt_dfstemp_u11, max(bflt_coiltemp_u12) as dvc_bflt_dfstemp_u12, max(bocflt_ef_u21) as dvc_bocflt_ef_u21, max(bocflt_ef_u22) as dvc_bocflt_ef_u22, max(bocflt_cf_u21) as dvc_bocflt_cf_u21, max(bocflt_cf_u22) as dvc_bocflt_cf_u22, max(bflt_vfd_u21) as dvc_bflt_vfd_u21, max(blpflt_comp_u21) as dvc_blpflt_comp_u21, max(bscflt_comp_u21) as dvc_bscflt_comp_u21, max(bflt_vfd_u22) as dvc_bflt_vfd_u22, max(blpflt_comp_u22) as dvc_blpflt_comp_u22, max(bscflt_comp_u22) as dvc_bscflt_comp_u22, max(fadpos_u2) as dvc_bflt_fad_u2, max(radpos_u2) as dvc_bflt_rad_u2, max(bflt_ap_u21) as dvc_bflt_airclean_u2, max(bflt_expboard_u1) as dvc_bflt_expboard_u2, max(bflt_frstemp_u2) as dvc_bflt_frstemp_u2, max(bflt_splytemp_u21) as dvc_bflt_splytemp_u21, max(bflt_splytemp_u22) as dvc_bflt_splytemp_u22, max(bflt_rnttemp_u2) as dvc_bflt_rnttemp_u2, max(bflt_coiltemp_u21) as dvc_bflt_dfstemp_u21, max(bflt_coiltemp_u22) as dvc_bflt_dfstemp_u22, max(bflt_vehtemp_u1) as dvc_bflt_vehtemp, max(bflt_vehtemp_u2) as dvc_bflt_seattemp, max(bflt_emergivt) as dvc_bflt_emergivt, max(bflt_vfd_com_u11) as dvc_bcomuflt_vfd_u11, max(bflt_vfd_com_u12) as dvc_bcomuflt_vfd_u12, max(bflt_vfd_com_u21) as dvc_bcomuflt_vfd_u21, max(bflt_vfd_com_u22) as dvc_bcomuflt_vfd_u22, max(bflt_powersupply_u1) as dvc_bmcbflt_pwr_u1, max(bflt_powersupply_u2) as dvc_bmcbflt_pwr_u2 FROM dev_macda WHERE msg_calc_parse_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
        else:
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_dvc_time, msg_calc_dvc_time) as time, max(bocflt_ef_u11) as dvc_bocflt_ef_u11, max(bocflt_ef_u12) as dvc_bocflt_ef_u12, max(bocflt_cf_u11) as dvc_bocflt_cf_u11, max(bocflt_cf_u12) as dvc_bocflt_cf_u12, max(bflt_vfd_u11) as dvc_bflt_vfd_u11, max(blpflt_comp_u11) as dvc_blpflt_comp_u11, max(bscflt_comp_u11) as dvc_bscflt_comp_u11, max(bflt_vfd_u12) as dvc_bflt_vfd_u12, max(blpflt_comp_u12) as dvc_blpflt_comp_u12, max(bscflt_comp_u12) as dvc_bscflt_comp_u12, max(fadpos_u1) as dvc_bflt_fad_u1, max(radpos_u2) as dvc_bflt_rad_u1, max(bflt_ap_u11) as dvc_bflt_airclean_u1, max(bflt_expboard_u1) as dvc_bflt_expboard_u1, max(bflt_frstemp_u1) as dvc_bflt_frstemp_u1, max(bflt_splytemp_u11) as dvc_bflt_splytemp_u11, max(bflt_splytemp_u12) as dvc_bflt_splytemp_u12, max(bflt_rnttemp_u1) as dvc_bflt_rnttemp_u1, max(bflt_coiltemp_u11) as dvc_bflt_dfstemp_u11, max(bflt_coiltemp_u12) as dvc_bflt_dfstemp_u12, max(bocflt_ef_u21) as dvc_bocflt_ef_u21, max(bocflt_ef_u22) as dvc_bocflt_ef_u22, max(bocflt_cf_u21) as dvc_bocflt_cf_u21, max(bocflt_cf_u22) as dvc_bocflt_cf_u22, max(bflt_vfd_u21) as dvc_bflt_vfd_u21, max(blpflt_comp_u21) as dvc_blpflt_comp_u21, max(bscflt_comp_u21) as dvc_bscflt_comp_u21, max(bflt_vfd_u22) as dvc_bflt_vfd_u22, max(blpflt_comp_u22) as dvc_blpflt_comp_u22, max(bscflt_comp_u22) as dvc_bscflt_comp_u22, max(fadpos_u2) as dvc_bflt_fad_u2, max(radpos_u2) as dvc_bflt_rad_u2, max(bflt_ap_u21) as dvc_bflt_airclean_u2, max(bflt_expboard_u2) as dvc_bflt_expboard_u2, max(bflt_frstemp_u2) as dvc_bflt_frstemp_u2, max(bflt_splytemp_u21) as dvc_bflt_splytemp_u21, max(bflt_splytemp_u22) as dvc_bflt_splytemp_u22, max(bflt_rnttemp_u2) as dvc_bflt_rnttemp_u2, max(bflt_coiltemp_u21) as dvc_bflt_dfstemp_u21, max(bflt_coiltemp_u22) as dvc_bflt_dfstemp_u22, max(bflt_vehtemp_u1) as dvc_bflt_vehtemp, max(bflt_vehtemp_u2) as dvc_bflt_seattemp, max(bflt_emergivt) as dvc_bflt_emergivt, max(bflt_vfd_com_u11) as dvc_bcomuflt_vfd_u11, max(bflt_vfd_com_u12) as dvc_bcomuflt_vfd_u12, max(bflt_vfd_com_u21) as dvc_bcomuflt_vfd_u21, max(bflt_vfd_com_u22) as dvc_bcomuflt_vfd_u22, max(bflt_powersupply_u1) as dvc_bmcbflt_pwr_u1, max(bflt_powersupply_u2) as dvc_bmcbflt_pwr_u2 FROM pro_macda WHERE msg_calc_dvc_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
        returndata = {}
        returndata['len'] = 0
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            if rlen >= 1:
                returndata['len'] = rlen
                returndata['data'] = result
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.get_fault_data() %s ' % exp)
            traceback.print_exc()
        return returndata

    def get_statis_data(self, mode):
        querysql = ''
        if mode == 'dev':
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_parse_time, msg_calc_parse_time) as time, last(dwef_op_tm_u11,msg_calc_parse_time) as dvc_dwoptime_ef_u1, last(dwcf_op_tm_u11,msg_calc_parse_time) as dvc_dwoptime_cf_u1, last(dwcp_op_tm_u11,msg_calc_parse_time) as dvc_dwoptime_comp_u11, last(dwcp_op_tm_u12,msg_calc_parse_time) as dvc_dwoptime_comp_u12, last(dwfad_op_cnt_u1 ,msg_calc_parse_time) as dvc_dwopcount_fad_u1, last(dwrad_op_cnt_u1,msg_calc_parse_time) as dvc_dwopcount_rad_u1, last(dwef_op_tm_u21,msg_calc_parse_time) as dvc_dwoptime_ef_u2, last(dwcf_op_tm_u21,msg_calc_parse_time) as dvc_dwoptime_cf_u2, last(dwcp_op_tm_u21,msg_calc_parse_time) as dvc_dwoptime_comp_u21, last(dwcp_op_tm_u22,msg_calc_parse_time) as dvc_dwoptime_comp_u22, last(dwfad_op_cnt_u2,msg_calc_parse_time) as dvc_dwopcount_fad_u2, last(dwrad_op_cnt_u2,msg_calc_parse_time) as dvc_dwopcount_rad_u2, last(dwexufan_op_tm,msg_calc_parse_time) as dvc_dwexufan_op_tm, last(dwdmpexu_op_cnt,msg_calc_parse_time) as dvc_dwdmpexu_op_cnt FROM dev_macda WHERE msg_calc_parse_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
        else:
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_dvc_time, msg_calc_dvc_time) as time, last(dwef_op_tm_u11,msg_calc_dvc_time) as dvc_dwoptime_ef_u1, last(dwcf_op_tm_u11,msg_calc_dvc_time) as dvc_dwoptime_cf_u1, last(dwcp_op_tm_u11,msg_calc_dvc_time) as dvc_dwoptime_comp_u11, last(dwcp_op_tm_u12,msg_calc_dvc_time) as dvc_dwoptime_comp_u12, last(dwfad_op_cnt_u1 ,msg_calc_dvc_time) as dvc_dwopcount_fad_u1, last(dwrad_op_cnt_u1,msg_calc_dvc_time) as dvc_dwopcount_rad_u1, last(dwef_op_tm_u21,msg_calc_dvc_time) as dvc_dwoptime_ef_u2, last(dwcf_op_tm_u21,msg_calc_dvc_time) as dvc_dwoptime_cf_u2, last(dwcp_op_tm_u21,msg_calc_dvc_time) as dvc_dwoptime_comp_u21, last(dwcp_op_tm_u22,msg_calc_dvc_time) as dvc_dwoptime_comp_u22, last(dwfad_op_cnt_u2,msg_calc_dvc_time) as dvc_dwopcount_fad_u2, last(dwrad_op_cnt_u2,msg_calc_dvc_time) as dvc_dwopcount_rad_u2, last(dwexufan_op_tm,msg_calc_dvc_time) as dvc_dwexufan_op_tm, last(dwdmpexu_op_cnt,msg_calc_dvc_time) as dvc_dwdmpexu_op_cnt FROM pro_macda WHERE msg_calc_dvc_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
        returndata = {}
        returndata['len'] = 0
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            if rlen >= 1:
                returndata['len'] = rlen
                returndata['data'] = result
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.get_fault_data() %s ' % exp)
            traceback.print_exc()
        return returndata

    def get_refdata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(wmode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(fas_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(f_cp_u11)) as dvc_w_freq_u11, approx_percentile(0.95, percentile_agg(suckp_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(suckp_u12)) as dvc_i_suck_pres_u12, approx_percentile(0.95, percentile_agg(wmode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(fas_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(f_cp_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(suckp_u21)) as dvc_i_suck_pres_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(suckp_u22)) as dvc_i_suck_pres_u22 " \
                   f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(wmode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(fas_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(f_cp_u11)) as dvc_w_freq_u11, approx_percentile(0.95, percentile_agg(suckp_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(suckp_u12)) as dvc_i_suck_pres_u12, approx_percentile(0.95, percentile_agg(wmode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(fas_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(f_cp_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(suckp_u21)) as dvc_i_suck_pres_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(suckp_u22)) as dvc_i_suck_pres_u22 " \
                   f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_refdata() %s ' % exp)
            traceback.print_exc()

    def get_pumpdata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, avg(ABS(f_cp_u11 - f_cp_u12)) as w_frequ1_sub, avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, avg(ABS(f_cp_u21 - f_cp_u22)) as w_frequ2_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, avg(ABS(f_cp_u11 - f_cp_u12)) as w_frequ1_sub, avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, avg(ABS(dvc_f_cp_u21 - f_cp_u22)) as w_frequ2_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub " \
                       f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_pumpdata() %s ' % exp)
            traceback.print_exc()

    def get_sensordata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, 1 as dvc_bflt_trainmove, avg(ABS(fas_u1 - fas_u2)) as fat_sub, avg(ABS(ras_u1 - ras_u2)) as rat_sub " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, 1 as dvc_bflt_trainmove, avg(ABS(fas_u1 - fas_u2)) as fat_sub, avg(ABS(ras_u1 - ras_u2)) as rat_sub " \
                       f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_sensordata() %s ' % exp)
            traceback.print_exc()

    # deprecated
    def get_predictdata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(wmode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(fas_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(f_cp_u11)) as dvc_w_freq_u11, " \
                       f"approx_percentile(0.95, percentile_agg(suckp_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(suckp_u12)) as dvc_i_suck_pres_u12, " \
                       f"approx_percentile(0.95, percentile_agg(wmode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(fas_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(f_cp_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(suckp_u21)) as dvc_i_suck_pres_u21, " \
                       f"approx_percentile(0.95, percentile_agg(f_cp_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(suckp_u22)) as dvc_i_suck_pres_u22, 1 as dvc_bflt_trainmove, avg(ABS(f_cp_u11 - f_cp_u12)) as w_frequ1_sub, avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, " \
                       f"avg(ABS(f_cp_u21 - f_cp_u22)) as w_frequ2_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub, avg(ABS(fas_u1 - fas_u2)) as fat_sub, avg(ABS(ras_u1 - ras_u2)) as rat_sub, approx_percentile(0.95, percentile_agg(bflt_tempover)) as dvc_bflt_cabinovertemp " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '5 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(wmode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(fas_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(f_cp_u11)) as dvc_w_freq_u11, " \
                       f"approx_percentile(0.95, percentile_agg(suckp_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(suckp_u12)) as dvc_i_suck_pres_u12, " \
                       f"approx_percentile(0.95, percentile_agg(wmode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(fas_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(f_cp_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(suckp_u21)) as dvc_i_suck_pres_u21, " \
                       f"approx_percentile(0.95, percentile_agg(f_cp_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(suckp_u22)) as dvc_i_suck_pres_u22, 1 as dvc_bflt_trainmove, avg(ABS(f_cp_u11 - f_cp_u12)) as w_frequ1_sub, avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, " \
                       f"avg(ABS(f_cp_u21 - f_cp_u22)) as w_frequ2_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub, avg(ABS(fas_u1 - fas_u2)) as fat_sub, avg(ABS(ras_u1 - ras_u2)) as rat_sub, approx_percentile(0.95, percentile_agg(bflt_tempover)) as dvc_bflt_cabinovertemp " \
                       f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '5 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_predictdata() %s ' % exp)
            traceback.print_exc()

    def prepare_predictdata(self, mode, dvc_no):
        querysql_3m = ''
        querysql_5m = ''
        querysql_10m = ''
        querysql_15m = ''
        querysql_20m = ''
        querysql_30m = ''
        if mode == 'dev':
            querysql_3m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                       f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                       f"avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '3 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_5m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                          f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                          f"approx_percentile(0.95, percentile_agg(suckp_u11)) as suckp_u11, approx_percentile(0.95, percentile_agg(suckp_u12)) as suckp_u12, approx_percentile(0.95, percentile_agg(suckp_u21)) as suckp_u21, approx_percentile(0.95, percentile_agg(suckp_u22)) as suckp_u22, " \
                          f"avg(ABS(fas_u1 - fas_u2)) as fas_sub, avg(ABS(ras_u1 - ras_u2)) as ras_sub " \
                          f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '5 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_10m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                           f"approx_percentile(0.95, percentile_agg(sp_u11)) as sp_u11, approx_percentile(0.95, percentile_agg(sp_u12)) as sp_u12, approx_percentile(0.95, percentile_agg(sp_u21)) as sp_u21, approx_percentile(0.95, percentile_agg(sp_u22)) as sp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, approx_percentile(0.95, percentile_agg(cfbk_cf_u11)) as cfbk_cf_u11, approx_percentile(0.95, percentile_agg(cfbk_cf_u21)) as cfbk_cf_u21, " \
                           f"approx_percentile(0.95, percentile_agg(i_ef_u11)) as i_ef_u11, approx_percentile(0.95, percentile_agg(i_ef_u12)) as i_ef_u12, approx_percentile(0.95, percentile_agg(i_ef_u21)) as i_ef_u21, approx_percentile(0.95, percentile_agg(i_ef_u22)) as i_ef_u22, " \
                           f"approx_percentile(0.95, percentile_agg(i_cf_u11)) as i_cf_u11, approx_percentile(0.95, percentile_agg(i_cf_u12)) as i_cf_u12, approx_percentile(0.95, percentile_agg(i_cf_u21)) as i_cf_u21, approx_percentile(0.95, percentile_agg(i_cf_u22)) as i_cf_u22, " \
                           f"approx_percentile(0.95, percentile_agg(fas_u1)) as fas_u1, approx_percentile(0.95, percentile_agg(fas_u2)) as fas_u2, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_exufan)) as cfbk_exufan, approx_percentile(0.95, percentile_agg(i_exufan)) as i_exufan, " \
                           f"approx_percentile(0.95, percentile_agg(i_cp_u11)) as i_cp_u11, approx_percentile(0.95, percentile_agg(i_cp_u12)) as i_cp_u12, approx_percentile(0.95, percentile_agg(i_cp_u21)) as i_cp_u21, approx_percentile(0.95, percentile_agg(i_cp_u22)) as i_cp_u22 " \
                           f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '10 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_15m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                          f"approx_percentile(0.95, percentile_agg(highpress_u11)) as highpress_u11, approx_percentile(0.95, percentile_agg(highpress_u12)) as highpress_u12, approx_percentile(0.95, percentile_agg(highpress_u21)) as highpress_u21, approx_percentile(0.95, percentile_agg(highpress_u22)) as highpress_u22 " \
                          f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '15 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_20m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                           f"approx_percentile(0.95, percentile_agg(bflt_tempover)) as bflt_tempover, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(aq_co2_u1)) as aq_co2_u1, approx_percentile(0.95, percentile_agg(aq_co2_u2)) as aq_co2_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_tvoc_u1)) as aq_tvoc_u1, approx_percentile(0.95, percentile_agg(aq_tvoc_u2)) as aq_tvoc_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm2_5_u1)) as aq_pm2_5_u1, approx_percentile(0.95, percentile_agg(aq_pm2_5_u2)) as aq_pm2_5_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm10_u1)) as aq_pm10_u1, approx_percentile(0.95, percentile_agg(aq_pm10_u2)) as aq_pm10_u2 " \
                           f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '20 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_30m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_cf_u11)) as cfbk_cf_u11, approx_percentile(0.95, percentile_agg(cfbk_cf_u21)) as cfbk_cf_u21, " \
                           f"approx_percentile(0.95, percentile_agg(presdiff_u1)) as presdiff_u1, approx_percentile(0.95, percentile_agg(presdiff_u2)) as presdiff_u2 " \
                           f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '30 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql_3m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                          f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                          f"avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub " \
                          f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '3 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_5m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                          f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                          f"approx_percentile(0.95, percentile_agg(suckp_u11)) as suckp_u11, approx_percentile(0.95, percentile_agg(suckp_u12)) as suckp_u12, approx_percentile(0.95, percentile_agg(suckp_u21)) as suckp_u21, approx_percentile(0.95, percentile_agg(suckp_u22)) as suckp_u22, " \
                          f"avg(ABS(fas_u1 - fas_u2)) as fas_sub, avg(ABS(ras_u1 - ras_u2)) as ras_sub " \
                          f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '5 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_10m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                           f"approx_percentile(0.95, percentile_agg(sp_u11)) as sp_u11, approx_percentile(0.95, percentile_agg(sp_u12)) as sp_u12, approx_percentile(0.95, percentile_agg(sp_u21)) as sp_u21, approx_percentile(0.95, percentile_agg(sp_u22)) as sp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, approx_percentile(0.95, percentile_agg(cfbk_cf_u11)) as cfbk_cf_u11, approx_percentile(0.95, percentile_agg(cfbk_cf_u21)) as cfbk_cf_u21, " \
                           f"approx_percentile(0.95, percentile_agg(i_ef_u11)) as i_ef_u11, approx_percentile(0.95, percentile_agg(i_ef_u12)) as i_ef_u12, approx_percentile(0.95, percentile_agg(i_ef_u21)) as i_ef_u21, approx_percentile(0.95, percentile_agg(i_ef_u22)) as i_ef_u22, " \
                           f"approx_percentile(0.95, percentile_agg(i_cf_u11)) as i_cf_u11, approx_percentile(0.95, percentile_agg(i_cf_u12)) as i_cf_u12, approx_percentile(0.95, percentile_agg(i_cf_u21)) as i_cf_u21, approx_percentile(0.95, percentile_agg(i_cf_u22)) as i_cf_u22, " \
                           f"approx_percentile(0.95, percentile_agg(fas_u1)) as fas_u1, approx_percentile(0.95, percentile_agg(fas_u2)) as fas_u2, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_exufan)) as cfbk_exufan, approx_percentile(0.95, percentile_agg(i_exufan)) as i_exufan, " \
                           f"approx_percentile(0.95, percentile_agg(i_cp_u11)) as i_cp_u11, approx_percentile(0.95, percentile_agg(i_cp_u12)) as i_cp_u12, approx_percentile(0.95, percentile_agg(i_cp_u21)) as i_cp_u21, approx_percentile(0.95, percentile_agg(i_cp_u22)) as i_cp_u22 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '10 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_15m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                           f"approx_percentile(0.95, percentile_agg(highpress_u11)) as highpress_u11, approx_percentile(0.95, percentile_agg(highpress_u12)) as highpress_u12, approx_percentile(0.95, percentile_agg(highpress_u21)) as highpress_u21, approx_percentile(0.95, percentile_agg(highpress_u22)) as highpress_u22 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '15 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_20m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                           f"approx_percentile(0.95, percentile_agg(bflt_tempover)) as bflt_tempover, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(aq_co2_u1)) as aq_co2_u1, approx_percentile(0.95, percentile_agg(aq_co2_u2)) as aq_co2_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_tvoc_u1)) as aq_tvoc_u1, approx_percentile(0.95, percentile_agg(aq_tvoc_u2)) as aq_tvoc_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm2_5_u1)) as aq_pm2_5_u1, approx_percentile(0.95, percentile_agg(aq_pm2_5_u2)) as aq_pm2_5_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm10_u1)) as aq_pm10_u1, approx_percentile(0.95, percentile_agg(aq_pm10_u2)) as aq_pm10_u2 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '20 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_30m = f"select msg_calc_dvc_no, round(approx_percentile(0.95, percentile_agg(wmode_u1))) as dvc_w_op_mode_u1, round(approx_percentile(0.95, percentile_agg(wmode_u2))) as dvc_w_op_mode_u2, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_cf_u11)) as cfbk_cf_u11, approx_percentile(0.95, percentile_agg(cfbk_cf_u21)) as cfbk_cf_u21, " \
                           f"approx_percentile(0.95, percentile_agg(presdiff_u1)) as presdiff_u1, approx_percentile(0.95, percentile_agg(presdiff_u2)) as presdiff_u2 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '30 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            #log.debug(querysql_3m)
            #log.debug(querysql_5m)
            #log.debug(querysql_10m)
            #log.debug(querysql_15m)
            #log.debug(querysql_20m)
            #log.debug(querysql_30m)
            returndata = {}
            returndata['len'] = 0
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            #query 3m
            cur.execute(querysql_3m)
            result = cur.fetchall()
            rlen = len(result)
            rdata = {}
            rdata['len'] = rlen
            if rlen >= 1:
                rdata['data'] = result[0]
                returndata['len'] += 1
            else:
                rdata['data'] = None
            returndata['data3m'] = rdata
            #query 5m
            cur.execute(querysql_5m)
            result = cur.fetchall()
            rlen = len(result)
            rdata = {}
            rdata['len'] = rlen
            if rlen >= 1:
                rdata['data'] = result[0]
                returndata['len'] += 1
            else:
                rdata['data'] = None
            returndata['data5m'] = rdata
            #query 10m
            cur.execute(querysql_10m)
            result = cur.fetchall()
            rlen = len(result)
            rdata = {}
            rdata['len'] = rlen
            if rlen >= 1:
                rdata['data'] = result[0]
                returndata['len'] += 1
            else:
                rdata['data'] = None
            returndata['data10m'] = rdata
            #query 15m
            cur.execute(querysql_15m)
            result = cur.fetchall()
            rlen = len(result)
            rdata = {}
            rdata['len'] = rlen
            if rlen >= 1:
                rdata['data'] = result[0]
                returndata['len'] += 1
            else:
                rdata['data'] = None
            returndata['data15m'] = rdata
            #query 20m
            cur.execute(querysql_20m)
            result = cur.fetchall()
            rlen = len(result)
            rdata = {}
            rdata['len'] = rlen
            if rlen >= 1:
                rdata['data'] = result[0]
                returndata['len'] += 1
            else:
                rdata['data'] = None
            returndata['data20m'] = rdata
            #query 30m
            cur.execute(querysql_30m)
            result = cur.fetchall()
            rlen = len(result)
            rdata = {}
            rdata['len'] = rlen
            if rlen >= 1:
                rdata['data'] = result[0]
                returndata['len'] += 1
            else:
                rdata['data'] = None
            returndata['data30m'] = rdata

            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.prepare_predictdata() %s ' % exp)
            traceback.print_exc()

    def predict(self, mode, dvc_no):
        predictdata = self.prepare_predictdata(mode, dvc_no)
        #log.debug(predictdata)
        predictjson = {}
        if predictdata['len'] == 6:
            predictsave = 0
            log.debug('Get Predict Data ... Success !')
            log.debug('Predict Start ... ...')
            # ref leak predict 冷媒泄露预警
            ref_leak_u11 = 0
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u1']) == 2 or round(predictdata['data5m']['data']['dvc_w_op_mode_u1']) ==3) and (round(predictdata['data5m']['data']['f_cp_u11'])>30 and round(predictdata['data5m']['data']['suckp_u11'])<2) :
                ref_leak_u11 = 1
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u1']) == 1) and (round(predictdata['data15m']['data']['highpress_u11'])<5) :
                ref_leak_u11 = 1
            ref_leak_u12 = 0
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u1']) == 2 or round(predictdata['data5m']['data']['dvc_w_op_mode_u1']) ==3) and (round(predictdata['data5m']['data']['f_cp_u12'])>30 and round(predictdata['data5m']['data']['suckp_u12'])<2) :
                ref_leak_u12 = 1
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u1']) == 1) and (round(predictdata['data15m']['data']['highpress_u12'])<5) :
                ref_leak_u12 = 1
            ref_leak_u21 = 0
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u2']) == 2 or round(predictdata['data5m']['data']['dvc_w_op_mode_u2']) ==3) and (round(predictdata['data5m']['data']['f_cp_u21'])>30 and round(predictdata['data5m']['data']['suckp_u21'])<2) :
                ref_leak_u21 = 1
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u2']) == 1) and (round(predictdata['data15m']['data']['highpress_u21'])<5) :
                ref_leak_u21 = 1
            ref_leak_u22 = 0
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u2']) == 2 or round(predictdata['data5m']['data']['dvc_w_op_mode_u2']) ==3) and (round(predictdata['data5m']['data']['f_cp_u22'])>30 and round(predictdata['data5m']['data']['suckp_u22'])<2) :
                ref_leak_u22 = 1
            if (round(predictdata['data5m']['data']['dvc_w_op_mode_u2']) == 1) and (round(predictdata['data15m']['data']['highpress_u22'])<5) :
                ref_leak_u22 = 1

            # f_cp predict 制冷系统预警
            f_cp_u1 = 0
            if (round(predictdata['data3m']['data']['f_cp_u11']) == round(predictdata['data3m']['data']['f_cp_u12'])) and (round(predictdata['data3m']['data']['w_crntu1_sub'],1) > 2):
                f_cp_u1 = 1
            if (round(predictdata['data10m']['data']['sp_u11'],1) > 20 or round(predictdata['data10m']['data']['sp_u11'],1) < -8 or round(predictdata['data10m']['data']['sp_u12'],1) > 20 or round(predictdata['data10m']['data']['sp_u12'],1) < -8):
                f_cp_u1 = 1
            f_cp_u2 = 0
            if (round(predictdata['data3m']['data']['f_cp_u21']) == round(predictdata['data3m']['data']['f_cp_u22'])) and (round(predictdata['data3m']['data']['w_crntu2_sub'],1) > 2):
                f_cp_u2 = 1
            if (round(predictdata['data10m']['data']['sp_u21'],1) > 20 or round(predictdata['data10m']['data']['sp_u21'],1) < -8 or round(predictdata['data10m']['data']['sp_u22'],1) > 20 or round(predictdata['data10m']['data']['sp_u22'],1) < -8):
                f_cp_u2 = 1

            # fas & ras predict 新风温度传感器 & 回风温度传感器 预警
            f_fas = 0
            if round(predictdata['data5m']['data']['fas_sub'],1) > 8 :
                f_fas = 1
            f_ras = 0
            if round(predictdata['data5m']['data']['ras_sub'], 1) > 8:
                f_ras = 1

            #cabin_overtemp predict 车厢温度超温预警
            cabin_overtemp = 0
            if round(predictdata['data20m']['data']['bflt_tempover'],1) > 0:
                cabin_overtemp = 1

            #f_presdiff  predict 滤网脏堵预警
            f_presdiff_u1 = 0
            if round(predictdata['data30m']['data']['cfbk_ef_u11']) ==1 and round(predictdata['data30m']['data']['presdiff_u1']) > 300:
                f_presdiff_u1 = 1
            f_presdiff_u2 = 0
            if round(predictdata['data30m']['data']['cfbk_ef_u21']) ==1 and round(predictdata['data30m']['data']['presdiff_u2']) > 300:
                f_presdiff_u2 = 1

            #f_ef predict 通风机电流预警
            f_ef_u11 = 0
            if round(predictdata['data10m']['data']['cfbk_ef_u11']) == 1 and round(predictdata['data10m']['data']['i_ef_u11'],1) > 2:
                f_ef_u11 = 1
            f_ef_u12 = 0
            if round(predictdata['data10m']['data']['cfbk_ef_u11']) == 1 and round(predictdata['data10m']['data']['i_ef_u12'],1) > 2:
                f_ef_u12 = 1
            f_ef_u21 = 0
            if round(predictdata['data10m']['data']['cfbk_ef_u21']) == 1 and round(predictdata['data10m']['data']['i_ef_u21'],1) > 2:
                f_ef_u21 = 1
            f_ef_u22 = 0
            if round(predictdata['data10m']['data']['cfbk_ef_u21']) == 1 and round(predictdata['data10m']['data']['i_ef_u22'],1) > 2:
                f_ef_u22 = 1

            #f_cf predict 冷凝风机电流预警
            f_cf_u11 = 0
            if round(predictdata['data10m']['data']['cfbk_cf_u11']) == 1 and round(
                    predictdata['data10m']['data']['i_cf_u11'], 1) > 2.9:
                f_cf_u11 = 1
            f_cf_u12 = 0
            if round(predictdata['data10m']['data']['cfbk_cf_u11']) == 1 and round(
                    predictdata['data10m']['data']['i_cf_u12'], 1) > 2.9:
                f_cf_u12 = 1
            f_cf_u21 = 0
            if round(predictdata['data10m']['data']['cfbk_cf_u21']) == 1 and round(
                    predictdata['data10m']['data']['i_cf_u21'], 1) > 2.9:
                f_cf_u21 = 1
            f_cf_u22 = 0
            if round(predictdata['data10m']['data']['cfbk_cf_u21']) == 1 and round(
                    predictdata['data10m']['data']['i_cf_u22'], 1) > 2.9:
                f_cf_u22 = 1

            # f_exufan predict 废排风机电流预警
            f_exufan = 0
            if round(predictdata['data10m']['data']['cfbk_exufan'] ==1) and round(predictdata['data10m']['data']['i_exufan'],1) > 4:
                f_exufan = 1

            # f_fas predict 压缩机电流预警
            f_fas_u11 = 0
            if round(predictdata['data10m']['data']['fas_u1'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u11'],1) > 18:
                f_fas_u11 = 1
            f_fas_u21 = 0
            if round(predictdata['data10m']['data']['fas_u1'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u12'],1) > 18:
                f_fas_u21 = 1
            f_fas_u12 = 0
            if round(predictdata['data10m']['data']['fas_u2'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u21'],1) > 18:
                f_fas_u12 = 1
            f_fas_u22 = 0
            if round(predictdata['data10m']['data']['fas_u2'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u22'],1) > 18:
                f_fas_u22 = 1

            # f_aq predict 空气质量监测终端预警
            f_aq_u1 = 0
            if (round(predictdata['data20m']['data']['cfbk_ef_u11']) == 1) and ( round(predictdata['data20m']['data']['aq_co2_u1'])>1200 or round(predictdata['data20m']['data']['aq_pm2_5_u1'])>75 or round(predictdata['data20m']['data']['aq_pm10_u1'])>150 or round(predictdata['data20m']['data']['aq_tvoc_u1'])>600):
                f_aq_u1 = 1
            f_aq_u2 = 0
            if (round(predictdata['data20m']['data']['cfbk_ef_u21']) == 1) and ( round(predictdata['data20m']['data']['aq_co2_u2'])>1200 or round(predictdata['data20m']['data']['aq_pm2_5_u2'])>75 or round(predictdata['data20m']['data']['aq_pm10_u2'])>150 or round(predictdata['data20m']['data']['aq_tvoc_u2'])>600):
                f_aq_u2 = 1

            predictsave = (f"{ref_leak_u11},{ref_leak_u12},{ref_leak_u21},{ref_leak_u22},{f_cp_u1},{f_cp_u2},{f_fas},{f_ras},{cabin_overtemp},{f_presdiff_u1},{f_presdiff_u2},"
                           f"{f_ef_u11},{f_ef_u12},{f_ef_u21},{f_ef_u22},{f_cf_u11},{f_cf_u12},{f_cf_u21},{f_cf_u22},{f_exufan},{f_fas_u11},{f_fas_u12},{f_fas_u21},{f_fas_u22},{f_aq_u1},{f_aq_u2}")
            predictskey = ['ref_leak_u11','ref_leak_u12','ref_leak_u21','ref_leak_u22','f_cp_u1','f_cp_u2','f_fas','f_ras','cabin_overtemp','f_presdiff_u1','f_presdiff_u2','f_ef_u11','f_ef_u12','f_ef_u21','f_ef_u22','f_cf_u11','f_cf_u12','f_cf_u21','f_cf_u22','f_exufan','f_fas_u11','f_fas_u12','f_fas_u21','f_fas_u22','f_aq_u1','f_aq_u2']
            for k in range(len(predictskey)):
                predictjson[predictskey[k]] = list(map(int,predictsave.split(',')))[k]
            #log.debug(predictskey)
            #log.debug(predictsave)
            #log.debug(list(map(int,predictsave.split(','))))
            #log.debug(sum(list(map(int,predictsave.split(',')))))
            log.debug(predictjson)
            log.debug('Predict ... Success !')
            return predictjson
        else:
            return predictjson

    def insert_predictdata(self, tablename, jsonobj):
        keylst = []
        valuelst = []
        masklst = []
        for (key, value) in jsonobj.items():
            keylst.append(key)
            valuelst.append(str(value))
            masklst.append('%s')
        keystr = ','.join(keylst)
        maskstr = ','.join(masklst)
        insertsql = f"INSERT INTO {tablename} ({keystr}) VALUES ({maskstr})"
        # log.debug(insertsql)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            cur.execute(insertsql, valuelst)
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.insert_predictdata() %s ' % exp)
            traceback.print_exc()

    def __del__(self):
        if self.conn_pool:
            self.conn_pool.closeall
        log.debug("PostgreSQL connection pool is closed")

    def parse_time(self, txt):
        date_s,time_s = txt.split(' ')
        year_s, mon_s, day_s = date_s.split('-')
        hour_s, minute_s, second_s = time_s.split(':')
        return datetime(int(year_s), int(mon_s), int(day_s), int(hour_s), int(minute_s), int(second_s))

if __name__ == '__main__':
    tu = TSutil()
    tu.predict('dev', '700203')
    '''
    tu = TSutil()
    jobj = {"schema":"s1","playload":"p1"}
    #tu.insert('dev_macda', jobj)
    
    result = tu.get_refdata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['msg_calc_dvc_no'])
    result = tu.get_pumpdata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['w_frequ1_sub'])
    result = tu.get_sensordata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['rat_sub'])

    result = tu.get_predictdata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['rat_sub'])
        log.debug(result)

    log.debug(tu.get_predict_data('dev'))
    log.debug(tu.get_fault_data('dev'))
    log.debug(tu.get_statis_data('dev'))
    #tu.get_refdata('pro', '5-98-1')
    pc = Counter()
    pc['ddd'] = 9
    if pc['aaa'] == 0:
        pc['aaa'] = 1
    pc['aaa'] += 1
    log.debug(pc['aaa'])
    log.debug(pc['ddd'])
    log.debug(pc['ccc'])
    import time

    now = int(round(time.time()*1000))

    print(now)
    '''
