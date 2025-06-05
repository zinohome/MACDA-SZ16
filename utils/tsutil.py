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
from psycopg2 import pool, sql
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
                dvc_flag INTEGER NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                dvc_year INTEGER NULL,
                dvc_month INTEGER NULL,
                dvc_day INTEGER NULL,
                dvc_hour INTEGER NULL,
                dvc_minute INTEGER NULL,
                dvc_second INTEGER NULL,
                dvc_op_condition INTEGER NULL,
                cfbk_ef_u11 INTEGER NULL,
                cfbk_cf_u11 INTEGER NULL,
                cfbk_cf_u12 INTEGER NULL,
                cfbk_comp_u11 INTEGER NULL,
                cfbk_comp_u12 INTEGER NULL,
                cfbk_ap_u11 INTEGER NULL,
                cfbk_ap_u12 INTEGER NULL,
                cfbk_ef_u21 INTEGER NULL,
                cfbk_cf_u21 INTEGER NULL,
                cfbk_cf_u22 INTEGER NULL,
                cfbk_comp_u21 INTEGER NULL,
                cfbk_comp_u22 INTEGER NULL,
                cfbk_ap_u21 INTEGER NULL,
                cfbk_ap_u22 INTEGER NULL,
                cfbk_tpp_u1 INTEGER NULL,
                cfbk_tpp_u2 INTEGER NULL,
                cfbk_ev_u1 INTEGER NULL,
                cfbk_ev_u2 INTEGER NULL,
                bocflt_ef_u11 INTEGER NULL,
                bocflt_ef_u12 INTEGER NULL,
                bocflt_cf_u11 INTEGER NULL,
                bocflt_cf_u12 INTEGER NULL,
                bflt_vfd_u11 INTEGER NULL,
                bflt_vfd_com_u11 INTEGER NULL,
                bflt_vfd_u12 INTEGER NULL,
                bflt_vfd_com_u12 INTEGER NULL,
                blpflt_comp_u11 INTEGER NULL,
                bscflt_comp_u11 INTEGER NULL,
                bscflt_vent_u11 INTEGER NULL,
                blpflt_comp_u12 INTEGER NULL,
                bscflt_comp_u12 INTEGER NULL,
                bscflt_vent_u12 INTEGER NULL,
                bflt_fad_u11 INTEGER NULL,
                bflt_fad_u12 INTEGER NULL,
                bflt_fad_u13 INTEGER NULL,
                bflt_fad_u14 INTEGER NULL,
                bflt_rad_u11 INTEGER NULL,
                bflt_rad_u12 INTEGER NULL,
                bflt_rad_u13 INTEGER NULL,
                bflt_rad_u14 INTEGER NULL,
                bflt_ap_u11 INTEGER NULL,
                bflt_ap_u12 INTEGER NULL,
                bflt_expboard_u1 INTEGER NULL,
                bflt_frstemp_u1 INTEGER NULL,
                bflt_rnttemp_u1 INTEGER NULL,
                bflt_splytemp_u11 INTEGER NULL,
                bflt_splytemp_u12 INTEGER NULL,
                bflt_insptemp_u11 INTEGER NULL,
                bflt_insptemp_u12 INTEGER NULL,
                bflt_lowpres_u11 INTEGER NULL,
                bflt_lowpres_u12 INTEGER NULL,
                bflt_highpres_u11 INTEGER NULL,
                bflt_highpres_u12 INTEGER NULL,
                bflt_diffpres_u1 INTEGER NULL,
                bocflt_ef_u21 INTEGER NULL,
                bocflt_ef_u22 INTEGER NULL,
                bocflt_cf_u21 INTEGER NULL,
                bocflt_cf_u22 INTEGER NULL,
                bflt_vfd_u21 INTEGER NULL,
                bflt_vfd_com_u21 INTEGER NULL,
                bflt_vfd_u22 INTEGER NULL,
                bflt_vfd_com_u22 INTEGER NULL,
                blpflt_comp_u21 INTEGER NULL,
                bscflt_comp_u21 INTEGER NULL,
                bscflt_vent_u21 INTEGER NULL,
                blpflt_comp_u22 INTEGER NULL,
                bscflt_comp_u22 INTEGER NULL,
                bscflt_vent_u22 INTEGER NULL,
                bflt_fad_u21 INTEGER NULL,
                bflt_fad_u22 INTEGER NULL,
                bflt_fad_u23 INTEGER NULL,
                bflt_fad_u24 INTEGER NULL,
                bflt_rad_u21 INTEGER NULL,
                bflt_rad_u22 INTEGER NULL,
                bflt_rad_u23 INTEGER NULL,
                bflt_rad_u24 INTEGER NULL,
                bflt_ap_u21 INTEGER NULL,
                bflt_ap_u22 INTEGER NULL,
                bflt_expboard_u2 INTEGER NULL,
                bflt_frstemp_u2 INTEGER NULL,
                bflt_rnttemp_u2 INTEGER NULL,
                bflt_splytemp_u21 INTEGER NULL,
                bflt_splytemp_u22 INTEGER NULL,
                bflt_insptemp_u21 INTEGER NULL,
                bflt_insptemp_u22 INTEGER NULL,
                bflt_lowpres_u21 INTEGER NULL,
                bflt_lowpres_u22 INTEGER NULL,
                bflt_highpres_u21 INTEGER NULL,
                bflt_highpres_u22 INTEGER NULL,
                bflt_diffpres_u2 INTEGER NULL,
                bflt_emergivt INTEGER NULL,
                bflt_vehtemp_u1 INTEGER NULL,
                bflt_vehhum_u1 INTEGER NULL,
                bflt_vehtemp_u2 INTEGER NULL,
                bflt_vehhum_u2 INTEGER NULL,
                bflt_airmon_u1 INTEGER NULL,
                bflt_airmon_u2 INTEGER NULL,
                bflt_currentmon INTEGER NULL,
                bflt_tcms INTEGER NULL,
                bscflt_ef_u1 INTEGER NULL,
                bscflt_cf_u1 INTEGER NULL,
                bscflt_vfd_pw_u11 INTEGER NULL,
                bscflt_vfd_pw_u12 INTEGER NULL,
                bscflt_ef_u2 INTEGER NULL,
                bscflt_cf_u2 INTEGER NULL,
                bscflt_vfd_pw_u21 INTEGER NULL,
                bscflt_vfd_pw_u22 INTEGER NULL,
                bflt_ef_cnt_u1 INTEGER NULL,
                bflt_cf_cnt_u11 INTEGER NULL,
                bflt_cf_cnt_u12 INTEGER NULL,
                bflt_vfd_cnt_u11 INTEGER NULL,
                bflt_vfd_cnt_u12 INTEGER NULL,
                bflt_ev_cnt_u1 INTEGER NULL,
                bflt_ef_cnt_u2 INTEGER NULL,
                bflt_cf_cnt_u21 INTEGER NULL,
                bflt_cf_cnt_u22 INTEGER NULL,
                bflt_vfd_cnt_u21 INTEGER NULL,
                bflt_vfd_cnt_u22 INTEGER NULL,
                bflt_ev_cnt_u2 INTEGER NULL,
                bflt_tempover INTEGER NULL,
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
                dvc_flag INTEGER NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                dvc_year INTEGER NULL,
                dvc_month INTEGER NULL,
                dvc_day INTEGER NULL,
                dvc_hour INTEGER NULL,
                dvc_minute INTEGER NULL,
                dvc_second INTEGER NULL,
                dvc_op_condition INTEGER NULL,
                cfbk_ef_u11 INTEGER NULL,
                cfbk_cf_u11 INTEGER NULL,
                cfbk_cf_u12 INTEGER NULL,
                cfbk_comp_u11 INTEGER NULL,
                cfbk_comp_u12 INTEGER NULL,
                cfbk_ap_u11 INTEGER NULL,
                cfbk_ap_u12 INTEGER NULL,
                cfbk_ef_u21 INTEGER NULL,
                cfbk_cf_u21 INTEGER NULL,
                cfbk_cf_u22 INTEGER NULL,
                cfbk_comp_u21 INTEGER NULL,
                cfbk_comp_u22 INTEGER NULL,
                cfbk_ap_u21 INTEGER NULL,
                cfbk_ap_u22 INTEGER NULL,
                cfbk_tpp_u1 INTEGER NULL,
                cfbk_tpp_u2 INTEGER NULL,
                cfbk_ev_u1 INTEGER NULL,
                cfbk_ev_u2 INTEGER NULL,
                bocflt_ef_u11 INTEGER NULL,
                bocflt_ef_u12 INTEGER NULL,
                bocflt_cf_u11 INTEGER NULL,
                bocflt_cf_u12 INTEGER NULL,
                bflt_vfd_u11 INTEGER NULL,
                bflt_vfd_com_u11 INTEGER NULL,
                bflt_vfd_u12 INTEGER NULL,
                bflt_vfd_com_u12 INTEGER NULL,
                blpflt_comp_u11 INTEGER NULL,
                bscflt_comp_u11 INTEGER NULL,
                bscflt_vent_u11 INTEGER NULL,
                blpflt_comp_u12 INTEGER NULL,
                bscflt_comp_u12 INTEGER NULL,
                bscflt_vent_u12 INTEGER NULL,
                bflt_fad_u11 INTEGER NULL,
                bflt_fad_u12 INTEGER NULL,
                bflt_fad_u13 INTEGER NULL,
                bflt_fad_u14 INTEGER NULL,
                bflt_rad_u11 INTEGER NULL,
                bflt_rad_u12 INTEGER NULL,
                bflt_rad_u13 INTEGER NULL,
                bflt_rad_u14 INTEGER NULL,
                bflt_ap_u11 INTEGER NULL,
                bflt_ap_u12 INTEGER NULL,
                bflt_expboard_u1 INTEGER NULL,
                bflt_frstemp_u1 INTEGER NULL,
                bflt_rnttemp_u1 INTEGER NULL,
                bflt_splytemp_u11 INTEGER NULL,
                bflt_splytemp_u12 INTEGER NULL,
                bflt_insptemp_u11 INTEGER NULL,
                bflt_insptemp_u12 INTEGER NULL,
                bflt_lowpres_u11 INTEGER NULL,
                bflt_lowpres_u12 INTEGER NULL,
                bflt_highpres_u11 INTEGER NULL,
                bflt_highpres_u12 INTEGER NULL,
                bflt_diffpres_u1 INTEGER NULL,
                bocflt_ef_u21 INTEGER NULL,
                bocflt_ef_u22 INTEGER NULL,
                bocflt_cf_u21 INTEGER NULL,
                bocflt_cf_u22 INTEGER NULL,
                bflt_vfd_u21 INTEGER NULL,
                bflt_vfd_com_u21 INTEGER NULL,
                bflt_vfd_u22 INTEGER NULL,
                bflt_vfd_com_u22 INTEGER NULL,
                blpflt_comp_u21 INTEGER NULL,
                bscflt_comp_u21 INTEGER NULL,
                bscflt_vent_u21 INTEGER NULL,
                blpflt_comp_u22 INTEGER NULL,
                bscflt_comp_u22 INTEGER NULL,
                bscflt_vent_u22 INTEGER NULL,
                bflt_fad_u21 INTEGER NULL,
                bflt_fad_u22 INTEGER NULL,
                bflt_fad_u23 INTEGER NULL,
                bflt_fad_u24 INTEGER NULL,
                bflt_rad_u21 INTEGER NULL,
                bflt_rad_u22 INTEGER NULL,
                bflt_rad_u23 INTEGER NULL,
                bflt_rad_u24 INTEGER NULL,
                bflt_ap_u21 INTEGER NULL,
                bflt_ap_u22 INTEGER NULL,
                bflt_expboard_u2 INTEGER NULL,
                bflt_frstemp_u2 INTEGER NULL,
                bflt_rnttemp_u2 INTEGER NULL,
                bflt_splytemp_u21 INTEGER NULL,
                bflt_splytemp_u22 INTEGER NULL,
                bflt_insptemp_u21 INTEGER NULL,
                bflt_insptemp_u22 INTEGER NULL,
                bflt_lowpres_u21 INTEGER NULL,
                bflt_lowpres_u22 INTEGER NULL,
                bflt_highpres_u21 INTEGER NULL,
                bflt_highpres_u22 INTEGER NULL,
                bflt_diffpres_u2 INTEGER NULL,
                bflt_emergivt INTEGER NULL,
                bflt_vehtemp_u1 INTEGER NULL,
                bflt_vehhum_u1 INTEGER NULL,
                bflt_vehtemp_u2 INTEGER NULL,
                bflt_vehhum_u2 INTEGER NULL,
                bflt_airmon_u1 INTEGER NULL,
                bflt_airmon_u2 INTEGER NULL,
                bflt_currentmon INTEGER NULL,
                bflt_tcms INTEGER NULL,
                bscflt_ef_u1 INTEGER NULL,
                bscflt_cf_u1 INTEGER NULL,
                bscflt_vfd_pw_u11 INTEGER NULL,
                bscflt_vfd_pw_u12 INTEGER NULL,
                bscflt_ef_u2 INTEGER NULL,
                bscflt_cf_u2 INTEGER NULL,
                bscflt_vfd_pw_u21 INTEGER NULL,
                bscflt_vfd_pw_u22 INTEGER NULL,
                bflt_ef_cnt_u1 INTEGER NULL,
                bflt_cf_cnt_u11 INTEGER NULL,
                bflt_cf_cnt_u12 INTEGER NULL,
                bflt_vfd_cnt_u11 INTEGER NULL,
                bflt_vfd_cnt_u12 INTEGER NULL,
                bflt_ev_cnt_u1 INTEGER NULL,
                bflt_ef_cnt_u2 INTEGER NULL,
                bflt_cf_cnt_u21 INTEGER NULL,
                bflt_cf_cnt_u22 INTEGER NULL,
                bflt_vfd_cnt_u21 INTEGER NULL,
                bflt_vfd_cnt_u22 INTEGER NULL,
                bflt_ev_cnt_u2 INTEGER NULL,
                bflt_tempover INTEGER NULL,
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
            create_pro_predict_table = "CREATE TABLE IF NOT EXISTS pro_predict (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, dvc_train_no INTEGER NULL, dvc_carriage_no INTEGER NULL, ref_leak_u11 integer NOT NULL DEFAULT 0, ref_leak_u12 integer NOT NULL DEFAULT 0, ref_leak_u21 integer NOT NULL DEFAULT 0, ref_leak_u22 integer NOT NULL DEFAULT 0, f_cp_u1 integer NOT NULL DEFAULT 0, f_cp_u2 integer NOT NULL DEFAULT 0, f_fas integer NOT NULL DEFAULT 0, f_ras integer NOT NULL DEFAULT 0, cabin_overtemp integer NOT NULL DEFAULT 0, f_presdiff_u1 integer NOT NULL DEFAULT 0, f_presdiff_u2 integer NOT NULL DEFAULT 0, f_ef_u11 integer NOT NULL DEFAULT 0, f_ef_u12 integer NOT NULL DEFAULT 0, f_ef_u21 integer NOT NULL DEFAULT 0, f_ef_u22 integer NOT NULL DEFAULT 0, f_cf_u11 integer NOT NULL DEFAULT 0, f_cf_u12 integer NOT NULL DEFAULT 0, f_cf_u21 integer NOT NULL DEFAULT 0, f_cf_u22 integer NOT NULL DEFAULT 0, f_fas_u11 integer NOT NULL DEFAULT 0, f_fas_u12 integer NOT NULL DEFAULT 0, f_fas_u21 integer NOT NULL DEFAULT 0, f_fas_u22 integer NOT NULL DEFAULT 0, f_aq_u1 integer NOT NULL DEFAULT 0, f_aq_u2 integer NOT NULL DEFAULT 0);"
            create_dev_predict_table = "CREATE TABLE IF NOT EXISTS dev_predict (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, dvc_train_no INTEGER NULL, dvc_carriage_no INTEGER NULL, ref_leak_u11 integer NOT NULL DEFAULT 0, ref_leak_u12 integer NOT NULL DEFAULT 0, ref_leak_u21 integer NOT NULL DEFAULT 0, ref_leak_u22 integer NOT NULL DEFAULT 0, f_cp_u1 integer NOT NULL DEFAULT 0, f_cp_u2 integer NOT NULL DEFAULT 0, f_fas integer NOT NULL DEFAULT 0, f_ras integer NOT NULL DEFAULT 0, cabin_overtemp integer NOT NULL DEFAULT 0, f_presdiff_u1 integer NOT NULL DEFAULT 0, f_presdiff_u2 integer NOT NULL DEFAULT 0, f_ef_u11 integer NOT NULL DEFAULT 0, f_ef_u12 integer NOT NULL DEFAULT 0, f_ef_u21 integer NOT NULL DEFAULT 0, f_ef_u22 integer NOT NULL DEFAULT 0, f_cf_u11 integer NOT NULL DEFAULT 0, f_cf_u12 integer NOT NULL DEFAULT 0, f_cf_u21 integer NOT NULL DEFAULT 0, f_cf_u22 integer NOT NULL DEFAULT 0, f_fas_u11 integer NOT NULL DEFAULT 0, f_fas_u12 integer NOT NULL DEFAULT 0, f_fas_u21 integer NOT NULL DEFAULT 0, f_fas_u22 integer NOT NULL DEFAULT 0, f_aq_u1 integer NOT NULL DEFAULT 0, f_aq_u2 integer NOT NULL DEFAULT 0);"
            cur = conn.cursor()
            cur.execute(create_pro_table)
            cur.execute(create_dev_table)
            cur.execute(create_pro_json_table)
            cur.execute(create_dev_json_table)
            cur.execute(create_pro_predict_table)
            cur.execute(create_dev_predict_table)
            cur.execute("SELECT create_hypertable('pro_macda', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('pro_macda_json', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda_json', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('pro_predict', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_predict', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);")
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
            # 创建系统表
            create_sys_fields = """
                CREATE TABLE IF NOT EXISTS sys_fields (
                    field_name VARCHAR(255),
                    field_code VARCHAR(255) UNIQUE,
                    field_category VARCHAR(255)
                );
            """
            create_idx_sys_fields_field_code = """
                CREATE INDEX IF NOT EXISTS idx_sys_fields_field_code ON sys_fields(field_code);
            """
            insert_sys_fields = """
            WITH all_fields (field_name, field_code, field_category) AS (
                VALUES
                    ('Flag', 'dvc_flag', 'Basic'),
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
                    ('空气质量监测终端预警 U-2', 'f_aq_u2', 'predict')
            )
            INSERT INTO sys_fields (field_name, field_code, field_category)
            SELECT af.field_name, af.field_code, af.field_category
            FROM all_fields af
            WHERE NOT EXISTS (
                SELECT 1 FROM sys_fields s 
                WHERE s.field_name = af.field_name
            );
            """
            create_dev_macda_ac = """
                CREATE TABLE IF NOT EXISTS dev_macda_ac (
                    dvc_train_no INTEGER NOT NULL,
                    dvc_carriage_no INTEGER NOT NULL,
                    PRIMARY KEY (dvc_train_no, dvc_carriage_no)
                );
            """
            insert_dev_macda_ac = """
                INSERT INTO dev_macda_ac (dvc_train_no, dvc_carriage_no)
                    SELECT train_num, carriage_num
                    FROM
                        (SELECT generate_series(12101, 12120) AS train_num) AS trains,
                        (SELECT generate_series(1, 6) AS carriage_num) AS carriages
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dev_macda_ac
                        WHERE dvc_train_no = train_num AND dvc_carriage_no = carriage_num
                    );
            """
            create_pro_macda_ac = """
                CREATE TABLE IF NOT EXISTS pro_macda_ac (
                    dvc_train_no INTEGER NOT NULL,
                    dvc_carriage_no INTEGER NOT NULL,
                    PRIMARY KEY (dvc_train_no, dvc_carriage_no)
                );
            """
            insert_pro_macda_ac = """
                INSERT INTO pro_macda_ac (dvc_train_no, dvc_carriage_no)
                    SELECT train_num, carriage_num
                    FROM
                        (SELECT generate_series(12101, 12120) AS train_num) AS trains,
                        (SELECT generate_series(1, 6) AS carriage_num) AS carriages
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM pro_macda_ac
                        WHERE dvc_train_no = train_num AND dvc_carriage_no = carriage_num
                    );
            """
            cur.execute(create_sys_fields)
            cur.execute(create_idx_sys_fields_field_code)
            cur.execute(insert_sys_fields)
            cur.execute(create_dev_macda_ac)
            cur.execute(insert_dev_macda_ac)
            cur.execute(create_pro_macda_ac)
            cur.execute(insert_pro_macda_ac)
            conn.commit()
            # 创建 转置表
            create_dev_status_transposed = """
                CREATE TABLE IF NOT EXISTS dev_status_transposed (
                msg_calc_dvc_time TEXT NOT NULL,
                msg_calc_parse_time TIMESTAMPTZ NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value INTEGER NULL);
            """
            create_hyper_dev_status_transposed = """
                SELECT create_hypertable('dev_status_transposed', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            """
            create_rp_dev_status_transposed = """
                SELECT add_retention_policy('dev_status_transposed', INTERVAL '1 year', if_not_exists => true);
            """
            create_dev_param_transposed = """
                CREATE TABLE IF NOT EXISTS dev_param_transposed (
                msg_calc_dvc_time TEXT NOT NULL,
                msg_calc_parse_time TIMESTAMPTZ NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value DOUBLE PRECISION NULL);
            """
            create_hyper_dev_param_transposed = """
                SELECT create_hypertable('dev_param_transposed', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            """
            create_rp_dev_param_transposed = """
                SELECT add_retention_policy('dev_param_transposed', INTERVAL '1 year', if_not_exists => true);
            """
            create_dev_error_transposed = """
                CREATE TABLE IF NOT EXISTS dev_error_transposed (
                msg_calc_dvc_time TEXT NOT NULL,
                msg_calc_parse_time TIMESTAMPTZ NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value INTEGER NULL);
            """
            create_hyper_dev_error_transposed = """
                SELECT create_hypertable('dev_error_transposed', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            """
            create_rp_dev_error_transposed = """
                SELECT add_retention_policy('dev_error_transposed', INTERVAL '1 year', if_not_exists => true);
            """
            create_idx_dev_error_transposed_group_time = """
                CREATE INDEX IF NOT EXISTS idx_dev_error_transposed_group_time 
                    ON dev_error_transposed (
                        msg_calc_dvc_no, 
                        msg_calc_train_no, 
                        dvc_train_no, 
                        dvc_carriage_no, 
                        param_name, 
                        msg_calc_parse_time
                    );
            """
            create_dev_statistics_transposed = """
                CREATE TABLE IF NOT EXISTS dev_statistics_transposed (
                msg_calc_dvc_time TEXT NOT NULL,
                msg_calc_parse_time TIMESTAMPTZ NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value DOUBLE PRECISION NULL);
            """
            create_hyper_dev_statistics_transposed = """
                SELECT create_hypertable('dev_statistics_transposed', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            """
            create_rp_dev_statistics_transposed = """
                SELECT add_retention_policy('dev_statistics_transposed', INTERVAL '1 year', if_not_exists => true);
            """
            create_dev_predict_transposed = """
                CREATE TABLE IF NOT EXISTS dev_predict_transposed (
                msg_calc_dvc_time TEXT NOT NULL,
                msg_calc_parse_time TIMESTAMPTZ NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value INTEGER NULL);
            """
            create_hyper_dev_predict_transposed = """
                SELECT create_hypertable('dev_predict_transposed', 'msg_calc_parse_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            """
            create_rp_dev_predict_transposed = """
                SELECT add_retention_policy('dev_predict_transposed', INTERVAL '1 year', if_not_exists => true);
            """
            create_idx_dev_predict_group_time = """
            CREATE INDEX IF NOT EXISTS idx_dev_predict_group_time 
                ON dev_predict_transposed (
                    msg_calc_dvc_no, 
                    msg_calc_train_no, 
                    dvc_train_no, 
                    dvc_carriage_no, 
                    param_name, 
                    msg_calc_parse_time
                );
            """
            create_pro_param_transposed = """
                            CREATE TABLE IF NOT EXISTS pro_param_transposed (
                            msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                            msg_calc_parse_time TEXT NOT NULL,
                            msg_calc_dvc_no TEXT NOT NULL,
                            msg_calc_train_no TEXT NOT NULL,
                            dvc_train_no INTEGER NULL,
                            dvc_carriage_no INTEGER NULL,
                            param_name TEXT NULL,
                            param_value DOUBLE PRECISION NULL);
                        """
            create_hyper_pro_param_transposed = """
                            SELECT create_hypertable('pro_param_transposed', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
                        """
            create_rp_pro_param_transposed = """
                            SELECT add_retention_policy('pro_param_transposed', INTERVAL '1 year', if_not_exists => true);
                        """
            create_pro_status_transposed = """
                                        CREATE TABLE IF NOT EXISTS pro_status_transposed (
                                        msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                                        msg_calc_parse_time TEXT NOT NULL,
                                        msg_calc_dvc_no TEXT NOT NULL,
                                        msg_calc_train_no TEXT NOT NULL,
                                        dvc_train_no INTEGER NULL,
                                        dvc_carriage_no INTEGER NULL,
                                        param_name TEXT NULL,
                                        param_value INTEGER NULL);
                                    """
            create_hyper_pro_status_transposed = """
                                        SELECT create_hypertable('pro_status_transposed', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
                                    """
            create_rp_pro_status_transposed = """
                                        SELECT add_retention_policy('pro_status_transposed', INTERVAL '1 year', if_not_exists => true);
                                    """
            create_pro_error_transposed = """
                            CREATE TABLE IF NOT EXISTS pro_error_transposed (
                            msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                            msg_calc_parse_time TEXT NOT NULL,
                            msg_calc_dvc_no TEXT NOT NULL,
                            msg_calc_train_no TEXT NOT NULL,
                            dvc_train_no INTEGER NULL,
                            dvc_carriage_no INTEGER NULL,
                            param_name TEXT NULL,
                            param_value INTEGER NULL);
                        """
            create_hyper_pro_error_transposed = """
                            SELECT create_hypertable('pro_error_transposed', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
                        """
            create_rp_pro_error_transposed = """
                            SELECT add_retention_policy('pro_error_transposed', INTERVAL '1 year', if_not_exists => true);
                        """

            create_idx_pro_error_transposed_group_time = """
                CREATE INDEX IF NOT EXISTS idx_pro_error_transposed_group_time 
                    ON pro_error_transposed (
                        msg_calc_dvc_no, 
                        msg_calc_train_no, 
                        dvc_train_no, 
                        dvc_carriage_no, 
                        param_name, 
                        msg_calc_dvc_time
                    );
            """
            create_pro_statistics_transposed = """
                            CREATE TABLE IF NOT EXISTS pro_statistics_transposed (
                            msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                            msg_calc_parse_time TEXT NOT NULL,
                            msg_calc_dvc_no TEXT NOT NULL,
                            msg_calc_train_no TEXT NOT NULL,
                            dvc_train_no INTEGER NULL,
                            dvc_carriage_no INTEGER NULL,
                            param_name TEXT NULL,
                            param_value DOUBLE PRECISION NULL);
                        """
            create_hyper_pro_statistics_transposed = """
                            SELECT create_hypertable('pro_statistics_transposed', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
                        """
            create_rp_pro_statistics_transposed = """
                            SELECT add_retention_policy('pro_statistics_transposed', INTERVAL '1 year', if_not_exists => true);
                        """
            create_pro_predict_transposed = """
                            CREATE TABLE IF NOT EXISTS pro_predict_transposed (
                            msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                            msg_calc_parse_time TEXT NOT NULL,
                            msg_calc_dvc_no TEXT NOT NULL,
                            msg_calc_train_no TEXT NOT NULL,
                            dvc_train_no INTEGER NULL,
                            dvc_carriage_no INTEGER NULL,
                            param_name TEXT NULL,
                            param_value INTEGER NULL);
                        """
            create_hyper_pro_predict_transposed = """
                            SELECT create_hypertable('pro_predict_transposed', 'msg_calc_dvc_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
                        """
            create_rp_pro_predict_transposed = """
                            SELECT add_retention_policy('pro_predict_transposed', INTERVAL '1 year', if_not_exists => true);
                        """
            create_idx_pro_predict_group_time = """
            CREATE INDEX IF NOT EXISTS idx_pro_predict_group_time 
                ON pro_predict_transposed (
                    msg_calc_dvc_no, 
                    msg_calc_train_no, 
                    dvc_train_no, 
                    dvc_carriage_no, 
                    param_name, 
                    msg_calc_dvc_time
                );
            """
            create_equipment_management = """
            CREATE TABLE IF NOT EXISTS equipment_management (
                id SERIAL PRIMARY KEY,
                device_name VARCHAR(100) NOT NULL,
                baseline_data VARCHAR(50) NOT NULL,
                associated_data VARCHAR(100) NOT NULL,
                processing_measures VARCHAR(100) NOT NULL,
                health_threshold NUMERIC(3,2) NOT NULL,
                sub_health_threshold NUMERIC(3,2) NOT NULL
            );    
            """
            insert_equipment_management = """
            INSERT INTO equipment_management (
                device_name, baseline_data, associated_data, processing_measures,
                health_threshold, sub_health_threshold
            )
            SELECT '机组1通风机', '90000000', '通风机累计运行时间-U11', '更换风机轴承', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组1通风机')
            UNION ALL
            SELECT '机组2通风机', '90000000', '通风机累计运行时间-U21', '更换风机轴承', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组2通风机')
            UNION ALL
            SELECT '机组1冷凝风机1', '90000000', '冷凝风机累计运行时间-U11', '更换风机轴承', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组1冷凝风机1')
            UNION ALL
            SELECT '机组1冷凝风机2', '90000000', '冷凝风机累计运行时间-U12', '更换风机轴承', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组1冷凝风机2')
            UNION ALL
            SELECT '机组2冷凝风机1', '90000000', '冷凝风机累计运行时间-U21', '更换风机轴承', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组2冷凝风机1')
            UNION ALL
            SELECT '机组2冷凝风机2', '90000000', '冷凝风机累计运行时间-U22', '更换风机轴承', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组2冷凝风机2')
            UNION ALL
            SELECT '机组1压缩机1', '180000000', '压缩机累计运行时间-U11', '更换压缩机', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组1压缩机1')
            UNION ALL
            SELECT '机组1压缩机2', '180000000', '压缩机累计运行时间-U12', '更换压缩机', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组1压缩机2')
            UNION ALL
            SELECT '机组2压缩机1', '180000000', '压缩机累计运行时间-U21', '更换压缩机', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组2压缩机1')
            UNION ALL
            SELECT '机组2压缩机2', '180000000', '压缩机累计运行时间-U22', '更换压缩机', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组2压缩机2')
            UNION ALL
            SELECT '机组1新风阀', '10000000', '新风阀开关次数-U1', '更换风阀执行器', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组1新风阀')
            UNION ALL
            SELECT '机组1回风阀', '10000000', '回风阀开关次数-U1', '更换风阀执行器', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组1回风阀')
            UNION ALL
            SELECT '机组2新风阀', '10000000', '新风阀开关次数-U2', '更换风阀执行器', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组2新风阀')
            UNION ALL
            SELECT '机组2回风阀', '10000000', '回风阀开关次数-U2', '更换风阀执行器', 0.7, 0.85
            WHERE NOT EXISTS (SELECT 1 FROM equipment_management WHERE device_name = '机组2回风阀');   
            """
            cur.execute(create_dev_status_transposed)
            cur.execute(create_hyper_dev_status_transposed)
            cur.execute(create_rp_dev_status_transposed)
            cur.execute(create_dev_param_transposed)
            cur.execute(create_hyper_dev_param_transposed)
            cur.execute(create_rp_dev_param_transposed)
            cur.execute(create_dev_error_transposed)
            cur.execute(create_hyper_dev_error_transposed)
            cur.execute(create_rp_dev_error_transposed)
            cur.execute(create_idx_dev_error_transposed_group_time)
            cur.execute(create_dev_statistics_transposed)
            cur.execute(create_hyper_dev_statistics_transposed)
            cur.execute(create_rp_dev_statistics_transposed)
            cur.execute(create_dev_predict_transposed)
            cur.execute(create_hyper_dev_predict_transposed)
            cur.execute(create_rp_dev_predict_transposed)
            cur.execute(create_idx_dev_predict_group_time)
            cur.execute(create_pro_status_transposed)
            cur.execute(create_hyper_pro_status_transposed)
            cur.execute(create_rp_pro_status_transposed)
            cur.execute(create_pro_param_transposed)
            cur.execute(create_hyper_pro_param_transposed)
            cur.execute(create_rp_pro_param_transposed)
            cur.execute(create_pro_error_transposed)
            cur.execute(create_hyper_pro_error_transposed)
            cur.execute(create_idx_pro_error_transposed_group_time)
            cur.execute(create_rp_pro_error_transposed)
            cur.execute(create_pro_statistics_transposed)
            cur.execute(create_hyper_pro_statistics_transposed)
            cur.execute(create_rp_pro_statistics_transposed)
            cur.execute(create_pro_predict_transposed)
            cur.execute(create_hyper_pro_predict_transposed)
            cur.execute(create_idx_pro_predict_group_time)
            cur.execute(create_rp_pro_predict_transposed)
            cur.execute(create_equipment_management)
            cur.execute(insert_equipment_management)
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
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_parse_time, msg_calc_parse_time) as time, max(ref_leak_u11) as ref_leak_u11, max(ref_leak_u12) as ref_leak_u12, max(ref_leak_u21) as ref_leak_u21, max(ref_leak_u22) as ref_leak_u22, max(f_cp_u1) as f_cp_u1, max(f_cp_u2) as f_cp_u2, max(f_fas) as f_fas, max(f_ras) as f_ras, max(cabin_overtemp) as cabin_overtemp, max(f_presdiff_u1) as f_presdiff_u1, max(f_presdiff_u2) as f_presdiff_u2, max(f_ef_u11) as f_ef_u11, max(f_ef_u12) as f_ef_u12, max(f_ef_u21) as f_ef_u21, max(f_ef_u22) as f_ef_u22, max(f_cf_u11) as f_cf_u11, max(f_cf_u12) as f_cf_u12, max(f_cf_u21) as f_cf_u21, max(f_cf_u22) as f_cf_u22, max(f_fas_u11) as f_fas_u11, max(f_fas_u12) as f_fas_u12, max(f_fas_u21) as f_fas_u21, max(f_fas_u22) as f_fas_u22, max(f_aq_u1) as f_aq_u1, max(f_aq_u2) as f_aq_u2 FROM dev_predict WHERE msg_calc_parse_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
        else:
            querysql = f"SELECT msg_calc_dvc_no, last(msg_calc_dvc_time, msg_calc_dvc_time) as time, max(ref_leak_u11) as ref_leak_u11, max(ref_leak_u12) as ref_leak_u12, max(ref_leak_u21) as ref_leak_u21, max(ref_leak_u22) as ref_leak_u22, max(f_cp_u1) as f_cp_u1, max(f_cp_u2) as f_cp_u2, max(f_fas) as f_fas, max(f_ras) as f_ras, max(cabin_overtemp) as cabin_overtemp, max(f_presdiff_u1) as f_presdiff_u1, max(f_presdiff_u2) as f_presdiff_u2, max(f_ef_u11) as f_ef_u11, max(f_ef_u12) as f_ef_u12, max(f_ef_u21) as f_ef_u21, max(f_ef_u22) as f_ef_u22, max(f_cf_u11) as f_cf_u11, max(f_cf_u12) as f_cf_u12, max(f_cf_u21) as f_cf_u21, max(f_cf_u22) as f_cf_u22, max(f_fas_u11) as f_fas_u11, max(f_fas_u12) as f_fas_u12, max(f_fas_u21) as f_fas_u21, max(f_fas_u22) as f_fas_u22, max(f_aq_u1) as f_aq_u1, max(f_aq_u2) as f_aq_u2 FROM pro_predict WHERE msg_calc_dvc_time > now() - INTERVAL '5 minutes' group by msg_calc_dvc_no"
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
        querysql_1m = ''
        querysql_3m = ''
        querysql_5m = ''
        querysql_10m = ''
        querysql_15m = ''
        querysql_20m = ''
        querysql_30m = ''
        if mode == 'dev':
            querysql_1m = f"select msg_calc_dvc_no, " \
                       f"approx_percentile(0.95, percentile_agg(bflt_tempover)) as bflt_tempover " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '1 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_3m = f"select msg_calc_dvc_no, " \
                       f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                       f"avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '3 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_5m = f"select msg_calc_dvc_no, " \
                          f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                          f"approx_percentile(0.95, percentile_agg(suckp_u11)) as suckp_u11, approx_percentile(0.95, percentile_agg(suckp_u12)) as suckp_u12, approx_percentile(0.95, percentile_agg(suckp_u21)) as suckp_u21, approx_percentile(0.95, percentile_agg(suckp_u22)) as suckp_u22, " \
                          f"avg(ABS(fas_u1 - fas_u2)) as fas_sub, avg(ABS(ras_u1 - ras_u2)) as ras_sub " \
                          f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '5 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_10m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_comp_u11)) as cfbk_comp_u11, approx_percentile(0.95, percentile_agg(cfbk_comp_u12)) as cfbk_comp_u12, approx_percentile(0.95, percentile_agg(cfbk_comp_u21)) as cfbk_comp_u21, approx_percentile(0.95, percentile_agg(cfbk_comp_u22)) as cfbk_comp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(sp_u11)) as sp_u11, approx_percentile(0.95, percentile_agg(sp_u12)) as sp_u12, approx_percentile(0.95, percentile_agg(sp_u21)) as sp_u21, approx_percentile(0.95, percentile_agg(sp_u22)) as sp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, approx_percentile(0.95, percentile_agg(cfbk_cf_u11)) as cfbk_cf_u11, approx_percentile(0.95, percentile_agg(cfbk_cf_u12)) as cfbk_cf_u12, approx_percentile(0.95, percentile_agg(cfbk_cf_u21)) as cfbk_cf_u21, approx_percentile(0.95, percentile_agg(cfbk_cf_u22)) as cfbk_cf_u22, " \
                           f"approx_percentile(0.95, percentile_agg(i_ef_u11)) as i_ef_u11, approx_percentile(0.95, percentile_agg(i_ef_u12)) as i_ef_u12, approx_percentile(0.95, percentile_agg(i_ef_u21)) as i_ef_u21, approx_percentile(0.95, percentile_agg(i_ef_u22)) as i_ef_u22, " \
                           f"approx_percentile(0.95, percentile_agg(i_cf_u11)) as i_cf_u11, approx_percentile(0.95, percentile_agg(i_cf_u12)) as i_cf_u12, approx_percentile(0.95, percentile_agg(i_cf_u21)) as i_cf_u21, approx_percentile(0.95, percentile_agg(i_cf_u22)) as i_cf_u22, " \
                           f"approx_percentile(0.95, percentile_agg(fas_sys)) as fas_sys, " \
                           f"approx_percentile(0.95, percentile_agg(i_cp_u11)) as i_cp_u11, approx_percentile(0.95, percentile_agg(i_cp_u12)) as i_cp_u12, approx_percentile(0.95, percentile_agg(i_cp_u21)) as i_cp_u21, approx_percentile(0.95, percentile_agg(i_cp_u22)) as i_cp_u22 " \
                           f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '10 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_15m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(highpress_u11)) as highpress_u11, approx_percentile(0.95, percentile_agg(highpress_u12)) as highpress_u12, approx_percentile(0.95, percentile_agg(highpress_u21)) as highpress_u21, approx_percentile(0.95, percentile_agg(highpress_u22)) as highpress_u22, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(aq_co2_u1)) as aq_co2_u1, approx_percentile(0.95, percentile_agg(aq_co2_u2)) as aq_co2_u2 " \
                           f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '15 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_20m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(aq_co2_u1)) as aq_co2_u1, approx_percentile(0.95, percentile_agg(aq_co2_u2)) as aq_co2_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_tvoc_u1)) as aq_tvoc_u1, approx_percentile(0.95, percentile_agg(aq_tvoc_u2)) as aq_tvoc_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm2_5_u1)) as aq_pm2_5_u1, approx_percentile(0.95, percentile_agg(aq_pm2_5_u2)) as aq_pm2_5_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm10_u1)) as aq_pm10_u1, approx_percentile(0.95, percentile_agg(aq_pm10_u2)) as aq_pm10_u2 " \
                           f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '20 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_30m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(presdiff_u1)) as presdiff_u1, approx_percentile(0.95, percentile_agg(presdiff_u2)) as presdiff_u2 " \
                           f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '30 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql_1m = f"select msg_calc_dvc_no, " \
                          f"approx_percentile(0.95, percentile_agg(bflt_tempover)) as bflt_tempover " \
                          f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '1 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_3m = f"select msg_calc_dvc_no, " \
                          f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                          f"avg(ABS(i_cp_u11 - i_cp_u12)) as w_crntu1_sub, avg(ABS(i_cp_u21 - i_cp_u22)) as w_crntu2_sub " \
                          f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '3 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_5m = f"select msg_calc_dvc_no, " \
                          f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                          f"approx_percentile(0.95, percentile_agg(suckp_u11)) as suckp_u11, approx_percentile(0.95, percentile_agg(suckp_u12)) as suckp_u12, approx_percentile(0.95, percentile_agg(suckp_u21)) as suckp_u21, approx_percentile(0.95, percentile_agg(suckp_u22)) as suckp_u22, " \
                          f"avg(ABS(fas_u1 - fas_u2)) as fas_sub, avg(ABS(ras_u1 - ras_u2)) as ras_sub " \
                          f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '5 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_10m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_comp_u11)) as cfbk_comp_u11, approx_percentile(0.95, percentile_agg(cfbk_comp_u12)) as cfbk_comp_u12, approx_percentile(0.95, percentile_agg(cfbk_comp_u21)) as cfbk_comp_u21, approx_percentile(0.95, percentile_agg(cfbk_comp_u22)) as cfbk_comp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(sp_u11)) as sp_u11, approx_percentile(0.95, percentile_agg(sp_u12)) as sp_u12, approx_percentile(0.95, percentile_agg(sp_u21)) as sp_u21, approx_percentile(0.95, percentile_agg(sp_u22)) as sp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, approx_percentile(0.95, percentile_agg(cfbk_cf_u11)) as cfbk_cf_u11, approx_percentile(0.95, percentile_agg(cfbk_cf_u12)) as cfbk_cf_u12, approx_percentile(0.95, percentile_agg(cfbk_cf_u21)) as cfbk_cf_u21, approx_percentile(0.95, percentile_agg(cfbk_cf_u22)) as cfbk_cf_u22, " \
                           f"approx_percentile(0.95, percentile_agg(i_ef_u11)) as i_ef_u11, approx_percentile(0.95, percentile_agg(i_ef_u12)) as i_ef_u12, approx_percentile(0.95, percentile_agg(i_ef_u21)) as i_ef_u21, approx_percentile(0.95, percentile_agg(i_ef_u22)) as i_ef_u22, " \
                           f"approx_percentile(0.95, percentile_agg(i_cf_u11)) as i_cf_u11, approx_percentile(0.95, percentile_agg(i_cf_u12)) as i_cf_u12, approx_percentile(0.95, percentile_agg(i_cf_u21)) as i_cf_u21, approx_percentile(0.95, percentile_agg(i_cf_u22)) as i_cf_u22, " \
                           f"approx_percentile(0.95, percentile_agg(fas_sys)) as fas_sys, " \
                           f"approx_percentile(0.95, percentile_agg(i_cp_u11)) as i_cp_u11, approx_percentile(0.95, percentile_agg(i_cp_u12)) as i_cp_u12, approx_percentile(0.95, percentile_agg(i_cp_u21)) as i_cp_u21, approx_percentile(0.95, percentile_agg(i_cp_u22)) as i_cp_u22 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '10 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_15m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(f_cp_u11)) as f_cp_u11, approx_percentile(0.95, percentile_agg(f_cp_u12)) as f_cp_u12, approx_percentile(0.95, percentile_agg(f_cp_u21)) as f_cp_u21, approx_percentile(0.95, percentile_agg(f_cp_u22)) as f_cp_u22, " \
                           f"approx_percentile(0.95, percentile_agg(highpress_u11)) as highpress_u11, approx_percentile(0.95, percentile_agg(highpress_u12)) as highpress_u12, approx_percentile(0.95, percentile_agg(highpress_u21)) as highpress_u21, approx_percentile(0.95, percentile_agg(highpress_u22)) as highpress_u22, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(aq_co2_u1)) as aq_co2_u1, approx_percentile(0.95, percentile_agg(aq_co2_u2)) as aq_co2_u2 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '15 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_20m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(aq_co2_u1)) as aq_co2_u1, approx_percentile(0.95, percentile_agg(aq_co2_u2)) as aq_co2_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_tvoc_u1)) as aq_tvoc_u1, approx_percentile(0.95, percentile_agg(aq_tvoc_u2)) as aq_tvoc_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm2_5_u1)) as aq_pm2_5_u1, approx_percentile(0.95, percentile_agg(aq_pm2_5_u2)) as aq_pm2_5_u2, " \
                           f"approx_percentile(0.95, percentile_agg(aq_pm10_u1)) as aq_pm10_u1, approx_percentile(0.95, percentile_agg(aq_pm10_u2)) as aq_pm10_u2 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '20 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"

            querysql_30m = f"select msg_calc_dvc_no, " \
                           f"approx_percentile(0.95, percentile_agg(cfbk_ef_u11)) as cfbk_ef_u11, approx_percentile(0.95, percentile_agg(cfbk_ef_u21)) as cfbk_ef_u21, " \
                           f"approx_percentile(0.95, percentile_agg(presdiff_u1)) as presdiff_u1, approx_percentile(0.95, percentile_agg(presdiff_u2)) as presdiff_u2 " \
                           f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '30 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            #log.debug(querysql_1m)
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
            #query 1m
            cur.execute(querysql_1m)
            result = cur.fetchall()
            rlen = len(result)
            rdata = {}
            rdata['len'] = rlen
            if rlen >= 1:
                rdata['data'] = result[0]
                returndata['len'] += 1
            else:
                rdata['data'] = None
            returndata['data1m'] = rdata
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
        log.debug(predictdata)
        predictjson = {}
        if predictdata is not None:
            if predictdata['len'] == 7:
                predictsave = 0
                log.debug('Get Predict Data ... Success !')
                log.debug('Predict Start ... ...')
                # ref leak predict 冷媒泄露预警
                log.debug('ref leak predict 冷媒泄露预警')
                ref_leak_u11 = 0
                if (round(predictdata['data5m']['data']['f_cp_u11'])>30 and round(predictdata['data5m']['data']['suckp_u11'])<2) :
                    ref_leak_u11 = 1
                if (round(predictdata['data5m']['data']['f_cp_u11']) == 0) and (round(predictdata['data15m']['data']['highpress_u11'])<5) :
                    ref_leak_u11 = 1
                ref_leak_u12 = 0
                if (round(predictdata['data5m']['data']['f_cp_u12'])>30 and round(predictdata['data5m']['data']['suckp_u12'])<2) :
                    ref_leak_u12 = 1
                if (round(predictdata['data5m']['data']['f_cp_u12']) == 1) and (round(predictdata['data15m']['data']['highpress_u12'])<5) :
                    ref_leak_u12 = 1
                ref_leak_u21 = 0
                if (round(predictdata['data5m']['data']['f_cp_u21'])>30 and round(predictdata['data5m']['data']['suckp_u21'])<2) :
                    ref_leak_u21 = 1
                if (round(predictdata['data5m']['data']['f_cp_u21']) == 1) and (round(predictdata['data15m']['data']['highpress_u21'])<5) :
                    ref_leak_u21 = 1
                ref_leak_u22 = 0
                if (round(predictdata['data5m']['data']['f_cp_u22'])>30 and round(predictdata['data5m']['data']['suckp_u22'])<2) :
                    ref_leak_u22 = 1
                if (round(predictdata['data5m']['data']['f_cp_u22']) == 1) and (round(predictdata['data15m']['data']['highpress_u22'])<5) :
                    ref_leak_u22 = 1

                # f_cp predict 制冷系统预警
                log.debug('f_cp predict 制冷系统预警')
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
                log.debug('fas & ras predict 温度传感器预警 预警')
                f_fas = 0
                if round(predictdata['data5m']['data']['fas_sub'],1) > 8 :
                    f_fas = 1
                f_ras = 0
                if round(predictdata['data5m']['data']['ras_sub'], 1) > 8:
                    f_ras = 1

                #cabin_overtemp predict 车厢温度超温预警
                log.debug('cabin_overtemp predict 车厢温度超温预警')
                cabin_overtemp = 0
                if round(predictdata['data1m']['data']['bflt_tempover'],1) > 0:
                    cabin_overtemp = 1

                #f_presdiff  predict 滤网脏堵预警
                log.debug('f_presdiff  predict 滤网脏堵预警')
                f_presdiff_u1 = 0
                if round(predictdata['data30m']['data']['cfbk_ef_u11']) ==1 and round(predictdata['data30m']['data']['presdiff_u1']) > 300:
                    f_presdiff_u1 = 1
                f_presdiff_u2 = 0
                if round(predictdata['data30m']['data']['cfbk_ef_u21']) ==1 and round(predictdata['data30m']['data']['presdiff_u2']) > 300:
                    f_presdiff_u2 = 1

                #f_ef predict 通风机电流预警
                log.debug('f_ef predict 通风机电流预警')
                f_ef_u11 = 0
                if round(predictdata['data10m']['data']['cfbk_ef_u11']) == 1 and round(predictdata['data10m']['data']['i_ef_u11'],1) > 2.2:
                    f_ef_u11 = 1
                f_ef_u12 = 0
                if round(predictdata['data10m']['data']['cfbk_ef_u11']) == 1 and round(predictdata['data10m']['data']['i_ef_u12'],1) > 2.2:
                    f_ef_u12 = 1
                f_ef_u21 = 0
                if round(predictdata['data10m']['data']['cfbk_ef_u21']) == 1 and round(predictdata['data10m']['data']['i_ef_u21'],1) > 2.2:
                    f_ef_u21 = 1
                f_ef_u22 = 0
                if round(predictdata['data10m']['data']['cfbk_ef_u21']) == 1 and round(predictdata['data10m']['data']['i_ef_u22'],1) > 2.2:
                    f_ef_u22 = 1

                #f_cf predict 冷凝风机电流预警
                log.debug('f_cf predict 冷凝风机电流预警')
                f_cf_u11 = 0
                if round(predictdata['data10m']['data']['cfbk_cf_u11']) == 1 and round(
                        predictdata['data10m']['data']['i_cf_u11'], 1) > 2.5:
                    f_cf_u11 = 1
                f_cf_u12 = 0
                if round(predictdata['data10m']['data']['cfbk_cf_u11']) == 1 and round(
                        predictdata['data10m']['data']['i_cf_u12'], 1) > 2.5:
                    f_cf_u12 = 1
                f_cf_u21 = 0
                if round(predictdata['data10m']['data']['cfbk_cf_u21']) == 1 and round(
                        predictdata['data10m']['data']['i_cf_u21'], 1) > 2.5:
                    f_cf_u21 = 1
                f_cf_u22 = 0
                if round(predictdata['data10m']['data']['cfbk_cf_u21']) == 1 and round(
                        predictdata['data10m']['data']['i_cf_u22'], 1) > 2.5:
                    f_cf_u22 = 1

                # f_fas predict 压缩机电流预警
                log.debug('f_fas predict 压缩机电流预警')
                f_fas_u11 = 0
                if round(predictdata['data10m']['data']['cfbk_comp_u11']) == 1 and round(predictdata['data10m']['data']['fas_sys'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u11'],1) > 18:
                    f_fas_u11 = 1
                f_fas_u21 = 0
                if round(predictdata['data10m']['data']['cfbk_comp_u12']) == 1 and round(predictdata['data10m']['data']['fas_sys'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u12'],1) > 18:
                    f_fas_u21 = 1
                f_fas_u12 = 0
                if round(predictdata['data10m']['data']['cfbk_comp_u21']) == 1 and round(predictdata['data10m']['data']['fas_sys'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u21'],1) > 18:
                    f_fas_u12 = 1
                f_fas_u22 = 0
                if round(predictdata['data10m']['data']['cfbk_comp_u22']) == 1 and round(predictdata['data10m']['data']['fas_sys'],1) < 35 and round(predictdata['data10m']['data']['i_cp_u22'],1) > 18:
                    f_fas_u22 = 1

                # f_aq predict 空气质量监测终端预警
                log.debug('f_aq predict 空气质量预警')
                f_aq_u1 = 0
                if (round(predictdata['data20m']['data']['cfbk_ef_u11']) == 1) and ( round(predictdata['data15m']['data']['aq_co2_u1'])>1200 or round(predictdata['data20m']['data']['aq_pm2_5_u1'])>75 or round(predictdata['data20m']['data']['aq_pm10_u1'])>150 or round(predictdata['data20m']['data']['aq_tvoc_u1'])>600):
                    f_aq_u1 = 1
                f_aq_u2 = 0
                if (round(predictdata['data20m']['data']['cfbk_ef_u21']) == 1) and ( round(predictdata['data15m']['data']['aq_co2_u2'])>1200 or round(predictdata['data20m']['data']['aq_pm2_5_u2'])>75 or round(predictdata['data20m']['data']['aq_pm10_u2'])>150 or round(predictdata['data20m']['data']['aq_tvoc_u2'])>600):
                    f_aq_u2 = 1

                predictsave = (f"{ref_leak_u11},{ref_leak_u12},{ref_leak_u21},{ref_leak_u22},{f_cp_u1},{f_cp_u2},{f_fas},{f_ras},{cabin_overtemp},{f_presdiff_u1},{f_presdiff_u2},"
                               f"{f_ef_u11},{f_ef_u12},{f_ef_u21},{f_ef_u22},{f_cf_u11},{f_cf_u12},{f_cf_u21},{f_cf_u22},{f_fas_u11},{f_fas_u12},{f_fas_u21},{f_fas_u22},{f_aq_u1},{f_aq_u2}")
                predictskey = ['ref_leak_u11','ref_leak_u12','ref_leak_u21','ref_leak_u22','f_cp_u1','f_cp_u2','f_fas','f_ras','cabin_overtemp','f_presdiff_u1','f_presdiff_u2','f_ef_u11','f_ef_u12','f_ef_u21','f_ef_u22','f_cf_u11','f_cf_u12','f_cf_u21','f_cf_u22','f_fas_u11','f_fas_u12','f_fas_u21','f_fas_u22','f_aq_u1','f_aq_u2']
                for k in range(len(predictskey)):
                    predictjson[predictskey[k]] = list(map(int,predictsave.split(',')))[k]
                #log.debug(predictskey)
                #log.debug(predictsave)
                #log.debug(list(map(int,predictsave.split(','))))
                #log.debug(sum(list(map(int,predictsave.split(',')))))
                #log.debug(predictjson)
                log.debug('Predict ... Success !')
                return predictjson
            else:
                return predictjson
        else:
            return predictjson

    def insert_predictdata(self, tablename, jsonobj):
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            # 参数映射表
            sys_fields_list = [
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
                ('空气质量监测终端预警 U-2', 'f_aq_u2', 'predict')
            ]

            # 为每个参数创建插入语句
            for param_name, param_code, param_category in sys_fields_list:
                if param_code in jsonobj:
                    param_value = jsonobj[param_code]

                    # 构建SQL插入语句
                    insert_query = sql.SQL("INSERT INTO "+tablename+"_transposed (msg_calc_dvc_time, msg_calc_parse_time, msg_calc_dvc_no, msg_calc_train_no, dvc_train_no, dvc_carriage_no,param_name, param_value)VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
                    # 执行插入
                    cur.execute(insert_query, (
                        jsonobj['msg_calc_dvc_time'], jsonobj['msg_calc_parse_time'], jsonobj['msg_calc_dvc_no'],
                        jsonobj['msg_calc_train_no'], jsonobj['dvc_train_no'], jsonobj['dvc_carriage_no'],
                        param_name, param_value
                    ))
            conn.commit()
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

    def refresh_materialized_view(self, view_name: str, concurrently: bool = True) -> None:
        """
        刷新PostgreSQL中的物化视图

        参数:
            view_name: 物化视图名称（带模式名，如 public.view_name）
            concurrently: 是否使用CONCURRENTLY选项（需要唯一索引）
        """
        conn = self.conn_pool.getconn()
        try:
            with conn.cursor() as cursor:
                # 拆分模式名和视图名
                parts = view_name.split('.')
                if len(parts) == 2:
                    schema_name, view_name = parts
                else:
                    schema_name = "public"
                    view_name = parts[0]
                # 构建刷新语句
                base_stmt = sql.SQL("REFRESH MATERIALIZED VIEW {} {}").format(
                    sql.SQL("CONCURRENTLY") if concurrently else sql.SQL(""),
                    sql.Identifier(schema_name, view_name)
                )
                cursor.execute(base_stmt)
                conn.commit()
                log.debug(f"成功刷新物化视图: {view_name}")
        except Exception as exp:
            log.error(f"刷新视图失败: {view_name}, 错误: {exp}")
            traceback.print_exc()
        finally:
            self.conn_pool.putconn(conn)  # 归还连接到池

    def refresh_all_materialized_views(self) -> None:
        """刷新所有物化视图"""
        views_to_refresh = [
            "public.dev_view_error_timed_mat",
            "public.pro_view_error_timed_mat",
            "public.dev_view_predict_timed_mat",
            "public.pro_view_predict_timed_mat"
            "public.dev_view_health_equipment_mat"
            "public.pro_view_health_equipment_mat"
        ]

        for view in views_to_refresh:
            try:
                self.refresh_materialized_view(view)
            except Exception as exp:
                log.error(f"刷新视图失败: {view}, 错误: {exp}")

if __name__ == '__main__':
    tu = TSutil()
    #tu.predict('dev', '1210106')
    tu.refresh_all_materialized_views()
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
