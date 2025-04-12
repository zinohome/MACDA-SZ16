# This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild
import time
import kaitaistruct
from kaitaistruct import KaitaiStruct, KaitaiStream, BytesIO
from core.settings import settings
from utils.log import log as log

if getattr(kaitaistruct, 'API_VERSION', (0, 9)) < (0, 9):
    raise Exception("Incompatible Kaitai Struct Python API: 0.9 or later is required, but you have %s" % (kaitaistruct.__version__))

div10list = ['fas_sys','ras_sys','tic','tveh_1','tveh_2','aq_t_u1','presdiff_u1','fas_u1','ras_u1','f_cp_u11','i_cp_u11','v_cp_u11','p_cp_u11','suckt_u11','suckp_u11','sp_u11','highpress_u11','sas_u11','ices_u11','f_cp_u12','i_cp_u12','v_cp_u12','p_cp_u12','suckt_u12','suckp_u12','sp_u12','highpress_u12','sas_u12','ices_u12','aq_t_u2','aq_h_u2','presdiff_u2','fas_u2','ras_u2','f_cp_u21','i_cp_u21','v_cp_u21','p_cp_u21','suckt_u21','suckp_u21','sp_u21','highpress_u21','sas_u21','ices_u21','f_cp_u22','i_cp_u22','v_cp_u22','p_cp_u22','suckt_u22','suckp_u22','sp_u22','highpress_u22','sas_u22','ices_u22','i_ef_u11','i_ef_u12','i_cf_u11','i_cf_u12','i_ef_u21','i_ef_u22','i_cf_u21','i_cf_u22','i_hvac_u1','i_hvac_u2','i_exufan']
div100list = ['']

class Nb67(KaitaiStruct):
    def __init__(self, _io, _parent=None, _root=None):
        self._io = _io
        self._parent = _parent
        self._root = _root if _root else self
        self._read()

    def _read(self):
        self.msg_header_code01 = self._io.read_u1()
        self.msg_header_code02 = self._io.read_u1()
        self.msg_length = self._io.read_u2be()
        self.msg_src_dvc_no = self._io.read_u1()
        self.msg_host_dvc_no = self._io.read_u1()
        self.msg_type = self._io.read_u2be()
        self.msg_frame_no = self._io.read_u2be()
        self.msg_line_no = self._io.read_u2be()
        self.msg_train_type = self._io.read_u2be()
        self.msg_train_no = self._io.read_u4be()
        self.msg_carriage_no = self._io.read_u1()
        self.msg_protocal_version = self._io.read_u1()
        self.msg_reversed1 = self._io.read_u2be()
        self.msg_reversed2 = self._io.read_u2be()
        self.msg_reversed3 = self._io.read_u2be()
        self.msg_reversed4 = self._io.read_u2be()
        self.msg_reversed5 = self._io.read_u2be()
        self.msg_src_dvc_year = self._io.read_u1()
        self.msg_src_dvc_month = self._io.read_u1()
        self.msg_src_dvc_day = self._io.read_u1()
        self.msg_src_dvc_hour = self._io.read_u1()
        self.msg_src_dvc_minute = self._io.read_u1()
        self.msg_src_dvc_second = self._io.read_u1()
        self.dvc_flag = self._io.read_u1()
        self.dvc_train_no = self._io.read_u2be()
        self.dvc_carriage_no = self._io.read_u1()
        self.dvc_year = self._io.read_u1()
        self.dvc_month = self._io.read_u1()
        self.dvc_day = self._io.read_u1()
        self.dvc_hour = self._io.read_u1()
        self.dvc_minute = self._io.read_u1()
        self.dvc_second = self._io.read_u1()
        self.ig_rsv0 = self._io.read_u1()
        self.ig_rsv1 = self._io.read_u1()
        self.cfbk_ef_u11 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv2 = self._io.read_bits_int_le(1) != 0
        self.cfbk_cf_u11 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv3 = self._io.read_bits_int_le(1) != 0
        self.cfbk_comp_u11 = self._io.read_bits_int_le(1) != 0
        self.cfbk_comp_u12 = self._io.read_bits_int_le(1) != 0
        self.cfbk_ap_u11 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv4 = self._io.read_bits_int_le(1) != 0
        self.cfbk_ef_u21 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv5 = self._io.read_bits_int_le(1) != 0
        self.cfbk_cf_u21 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv6 = self._io.read_bits_int_le(1) != 0
        self.cfbk_comp_u21 = self._io.read_bits_int_le(1) != 0
        self.cfbk_comp_u22 = self._io.read_bits_int_le(1) != 0
        self.cfbk_ap_u21 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv7 = self._io.read_bits_int_le(1) != 0
        self.cfbk_tpp_u1 = self._io.read_bits_int_le(1) != 0
        self.cfbk_tpp_u2 = self._io.read_bits_int_le(1) != 0
        self.cfbk_ev_u1 = self._io.read_bits_int_le(1) != 0
        self.cfbk_ev_u2 = self._io.read_bits_int_le(1) != 0
        self.cfbk_ewd = self._io.read_bits_int_le(1) != 0
        self.cfbk_exufan = self._io.read_bits_int_le(1) != 0
        self.ig_rsv9 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv10 = self._io.read_bits_int_le(1) != 0
        self.bocflt_ef_u11 = self._io.read_bits_int_le(1) != 0
        self.bocflt_ef_u12 = self._io.read_bits_int_le(1) != 0
        self.bocflt_cf_u11 = self._io.read_bits_int_le(1) != 0
        self.bocflt_cf_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_com_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_com_u12 = self._io.read_bits_int_le(1) != 0
        self.blpflt_comp_u11 = self._io.read_bits_int_le(1) != 0
        self.bscflt_comp_u11 = self._io.read_bits_int_le(1) != 0
        self.bscflt_vent_u11 = self._io.read_bits_int_le(1) != 0
        self.blpflt_comp_u12 = self._io.read_bits_int_le(1) != 0
        self.bscflt_comp_u12 = self._io.read_bits_int_le(1) != 0
        self.bscflt_vent_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_fad_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_fad_u12 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv11 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv12 = self._io.read_bits_int_le(1) != 0
        self.bflt_rad_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_rad_u12 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv13 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv14 = self._io.read_bits_int_le(1) != 0
        self.bflt_ap_u11 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv15 = self._io.read_bits_int_le(1) != 0
        self.bflt_expboard_u1 = self._io.read_bits_int_le(1) != 0
        self.bflt_frstemp_u1 = self._io.read_bits_int_le(1) != 0
        self.bflt_rnttemp_u1 = self._io.read_bits_int_le(1) != 0
        self.bflt_splytemp_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_splytemp_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_coiltemp_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_coiltemp_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_insptemp_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_insptemp_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_lowpres_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_lowpres_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_highpres_u11 = self._io.read_bits_int_le(1) != 0
        self.bflt_highpres_u12 = self._io.read_bits_int_le(1) != 0
        self.bflt_diffpres_u1 = self._io.read_bits_int_le(1) != 0
        self.bocflt_ef_u21 = self._io.read_bits_int_le(1) != 0
        self.bocflt_ef_u22 = self._io.read_bits_int_le(1) != 0
        self.bocflt_cf_u21 = self._io.read_bits_int_le(1) != 0
        self.bocflt_cf_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_com_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_vfd_com_u22 = self._io.read_bits_int_le(1) != 0
        self.blpflt_comp_u21 = self._io.read_bits_int_le(1) != 0
        self.bscflt_comp_u21 = self._io.read_bits_int_le(1) != 0
        self.bscflt_vent_u21 = self._io.read_bits_int_le(1) != 0
        self.blpflt_comp_u22 = self._io.read_bits_int_le(1) != 0
        self.bscflt_comp_u22 = self._io.read_bits_int_le(1) != 0
        self.bscflt_vent_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_fad_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_fad_u22 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv16 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv17 = self._io.read_bits_int_le(1) != 0
        self.bflt_rad_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_rad_u22 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv18 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv19 = self._io.read_bits_int_le(1) != 0
        self.bflt_ap_u21 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv20 = self._io.read_bits_int_le(1) != 0
        self.bflt_expboard_u2 = self._io.read_bits_int_le(1) != 0
        self.bflt_frstemp_u2 = self._io.read_bits_int_le(1) != 0
        self.bflt_rnttemp_u2 = self._io.read_bits_int_le(1) != 0
        self.bflt_splytemp_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_splytemp_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_coiltemp_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_coiltemp_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_insptemp_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_insptemp_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_lowpres_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_lowpres_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_highpres_u21 = self._io.read_bits_int_le(1) != 0
        self.bflt_highpres_u22 = self._io.read_bits_int_le(1) != 0
        self.bflt_diffpres_u2 = self._io.read_bits_int_le(1) != 0
        self.bflt_emergivt = self._io.read_bits_int_le(1) != 0
        self.ig_rsv240 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv241 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv242 = self._io.read_bits_int_le(1) != 0
        self.bflt_vehtemp_u1 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv251 = self._io.read_bits_int_le(1) != 0
        self.bflt_vehtemp_u2 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv252 = self._io.read_bits_int_le(1) != 0
        self.bflt_airmon_u1 = self._io.read_bits_int_le(1) != 0
        self.bflt_airmon_u2 = self._io.read_bits_int_le(1) != 0
        self.bflt_currentmon = self._io.read_bits_int_le(1) != 0
        self.bflt_tcms = self._io.read_bits_int_le(1) != 0
        self._io.align_to_byte()
        self.ig_rsv26 = self._io.read_u1()
        self.ig_rsv27 = self._io.read_u1()
        self.ig_rsv28 = self._io.read_u1()
        self.bflt_tempover = self._io.read_bits_int_le(1) != 0
        self.bflt_powersupply_u1 = self._io.read_bits_int_le(1) != 0
        self.bflt_powersupply_u2 = self._io.read_bits_int_le(1) != 0
        self.bflt_exhaustfan = self._io.read_bits_int_le(1) != 0
        self.bflt_exhaustval = self._io.read_bits_int_le(1) != 0
        self.ig_rsv29 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv30 = self._io.read_bits_int_le(1) != 0
        self.ig_rsv31 = self._io.read_bits_int_le(1) != 0
        self._io.align_to_byte()
        self.ig_rsv32 = self._io.read_s2be()
        self.ig_rsv33 = self._io.read_s2be()
        self.fas_sys = self._io.read_s2be()
        self.ras_sys = self._io.read_s2be()
        self.tic = self._io.read_s2be()
        self.load = self._io.read_s2be()
        self.wrsv_42 = self._io.read_s2be()
        self.tveh_1 = self._io.read_s2be()
        self.humdity_1 = self._io.read_s2be()
        self.tveh_2 = self._io.read_s2be()
        self.humdity_2 = self._io.read_s2be()
        self.aq_t_u1 = self._io.read_s2be()
        self.aq_h_u1 = self._io.read_s2be()
        self.aq_co2_u1 = self._io.read_s2be()
        self.aq_tvoc_u1 = self._io.read_s2be()
        self.aq_formald_u1 = self._io.read_s2be()
        self.aq_pm2_5_u1 = self._io.read_s2be()
        self.aq_pm10_u1 = self._io.read_s2be()
        self.aq_rsv_u1 = self._io.read_s2be()
        self.wmode_u1 = self._io.read_s2be()
        self.presdiff_u1 = self._io.read_s2be()
        self.fas_u1 = self._io.read_s2be()
        self.ras_u1 = self._io.read_s2be()
        self.fadpos_u1 = self._io.read_s2be()
        self.radpos_u1 = self._io.read_s2be()
        self.f_cp_u11 = self._io.read_s2be()
        self.i_cp_u11 = self._io.read_s2be()
        self.v_cp_u11 = self._io.read_s2be()
        self.p_cp_u11 = self._io.read_s2be()
        self.suckt_u11 = self._io.read_s2be()
        self.suckp_u11 = self._io.read_s2be()
        self.sp_u11 = self._io.read_s2be()
        self.eevpos_u11 = self._io.read_s2be()
        self.highpress_u11 = self._io.read_s2be()
        self.sas_u11 = self._io.read_s2be()
        self.ices_u11 = self._io.read_s2be()
        self.f_cp_u12 = self._io.read_s2be()
        self.i_cp_u12 = self._io.read_s2be()
        self.v_cp_u12 = self._io.read_s2be()
        self.p_cp_u12 = self._io.read_s2be()
        self.suckt_u12 = self._io.read_s2be()
        self.suckp_u12 = self._io.read_s2be()
        self.sp_u12 = self._io.read_s2be()
        self.eevpos_u12 = self._io.read_s2be()
        self.highpress_u12 = self._io.read_s2be()
        self.sas_u12 = self._io.read_s2be()
        self.ices_u12 = self._io.read_s2be()
        self.wrsv_124 = self._io.read_s2be()
        self.aq_t_u2 = self._io.read_s2be()
        self.aq_h_u2 = self._io.read_s2be()
        self.aq_co2_u2 = self._io.read_s2be()
        self.aq_tvoc_u2 = self._io.read_s2be()
        self.aq_formald_u2 = self._io.read_s2be()
        self.aq_pm2_5_u2 = self._io.read_s2be()
        self.aq_pm10_u2 = self._io.read_s2be()
        self.aq_rsv_u2 = self._io.read_s2be()
        self.wmode_u2 = self._io.read_s2be()
        self.presdiff_u2 = self._io.read_s2be()
        self.fas_u2 = self._io.read_s2be()
        self.ras_u2 = self._io.read_s2be()
        self.fadpos_u2 = self._io.read_s2be()
        self.radpos_u2 = self._io.read_s2be()
        self.f_cp_u21 = self._io.read_s2be()
        self.i_cp_u21 = self._io.read_s2be()
        self.v_cp_u21 = self._io.read_s2be()
        self.p_cp_u21 = self._io.read_s2be()
        self.suckt_u21 = self._io.read_s2be()
        self.suckp_u21 = self._io.read_s2be()
        self.sp_u21 = self._io.read_s2be()
        self.eevpos_u21 = self._io.read_s2be()
        self.highpress_u21 = self._io.read_s2be()
        self.sas_u21 = self._io.read_s2be()
        self.ices_u21 = self._io.read_s2be()
        self.f_cp_u22 = self._io.read_s2be()
        self.i_cp_u22 = self._io.read_s2be()
        self.v_cp_u22 = self._io.read_s2be()
        self.p_cp_u22 = self._io.read_s2be()
        self.suckt_u22 = self._io.read_s2be()
        self.suckp_u22 = self._io.read_s2be()
        self.sp_u22 = self._io.read_s2be()
        self.eevpos_u22 = self._io.read_s2be()
        self.highpress_u22 = self._io.read_s2be()
        self.sas_u22 = self._io.read_s2be()
        self.ices_u22 = self._io.read_s2be()
        self.ig_rsv34 = self._io.read_s2be()
        self.ig_rsv35 = self._io.read_s2be()
        self.ig_rsv36 = self._io.read_s2be()
        self.ig_rsv37 = self._io.read_s2be()
        self.i_ef_u11 = self._io.read_s2be()
        self.i_ef_u12 = self._io.read_s2be()
        self.i_cf_u11 = self._io.read_s2be()
        self.i_cf_u12 = self._io.read_s2be()
        self.i_ef_u21 = self._io.read_s2be()
        self.i_ef_u22 = self._io.read_s2be()
        self.i_cf_u21 = self._io.read_s2be()
        self.i_cf_u22 = self._io.read_s2be()
        self.i_hvac_u1 = self._io.read_s2be()
        self.i_hvac_u2 = self._io.read_s2be()
        self.i_exufan = self._io.read_s2be()
        self.ig_rsv38 = self._io.read_s2be()
        self.ig_rsv39 = self._io.read_s2be()
        self.dwpower = self._io.read_u4be()
        self.dwemerg_op_tm = self._io.read_u4be()
        self.dwemerg_op_cnt = self._io.read_u4be()
        self.dwef_op_tm_u11 = self._io.read_u4be()
        self.ig_rsv40 = self._io.read_u4be()
        self.dwcf_op_tm_u11 = self._io.read_u4be()
        self.ig_rsv41 = self._io.read_u4be()
        self.dwcp_op_tm_u11 = self._io.read_u4be()
        self.dwcp_op_tm_u12 = self._io.read_u4be()
        self.ig_rsv42 = self._io.read_u4be()
        self.ig_rsv43 = self._io.read_u4be()
        self.dwfad_op_cnt_u1 = self._io.read_u4be()
        self.dwrad_op_cnt_u1 = self._io.read_u4be()
        self.dwef_op_cnt_u11 = self._io.read_u4be()
        self.ig_rsv44 = self._io.read_u4be()
        self.dwcf_op_cnt_u11 = self._io.read_u4be()
        self.ig_rsv45 = self._io.read_u4be()
        self.dwcp_op_cnt_u11 = self._io.read_u4be()
        self.dwcp_op_cnt_u12 = self._io.read_u4be()
        self.ig_rsv46 = self._io.read_u4be()
        self.ig_rsv47 = self._io.read_u4be()
        self.dwef_op_tm_u21 = self._io.read_u4be()
        self.ig_rsv48 = self._io.read_u4be()
        self.dwcf_op_tm_u21 = self._io.read_u4be()
        self.ig_rsv49 = self._io.read_u4be()
        self.dwcp_op_tm_u21 = self._io.read_u4be()
        self.dwcp_op_tm_u22 = self._io.read_u4be()
        self.ig_rsv50 = self._io.read_u4be()
        self.ig_rsv51 = self._io.read_u4be()
        self.dwfad_op_cnt_u2 = self._io.read_u4be()
        self.dwrad_op_cnt_u2 = self._io.read_u4be()
        self.dwef_op_cnt_u21 = self._io.read_u4be()
        self.ig_rsv52 = self._io.read_u4be()
        self.dwcf_op_cnt_u21 = self._io.read_u4be()
        self.ig_rsv53 = self._io.read_u4be()
        self.dwcp_op_cnt_u21 = self._io.read_u4be()
        self.dwcp_op_cnt_u22 = self._io.read_u4be()
        self.ig_rsv54 = self._io.read_u4be()
        self.ig_rsv55 = self._io.read_u4be()
        self.dwexufan_op_tm = self._io.read_u4be()
        self.dwexufan_op_cnt = self._io.read_u4be()
        self.dwdmpexu_op_cnt = self._io.read_u4be()
        self.ig_rsv56 = self._io.read_u4be()
        self.ig_rsv57 = self._io.read_u4be()
        self.ig_rsv58 = self._io.read_u4be()
        self.ig_rsv59 = self._io.read_u4be()
        self.ig_rsv60 = self._io.read_u4be()
        self.ig_rsv61 = self._io.read_u4be()
        self.ig_rsv62 = self._io.read_u4be()
        self.ig_rsv63 = self._io.read_u4be()
        self.ig_rsv64 = self._io.read_u4be()
        self.ig_rsv65 = self._io.read_u4be()
        self.ig_rsv66 = self._io.read_u4be()
        self.ig_rsv67 = self._io.read_u4be()
        self.ig_rsv68 = self._io.read_u4be()
        self.ig_rsv69 = self._io.read_u4be()

    def from_file_to_dict(binfile):
        dev_mode = settings.DEV_MODE
        nb67dict = Nb67.from_file(binfile).__dict__.copy()
        lineno = int(str(nb67dict['dvc_train_no'])[:-3])
        trainno = int(str(nb67dict['dvc_train_no'])[-3:])
        nb67dict['msg_calc_train_no'] = f"{str(lineno)}{str(trainno).zfill(3)}"
        nb67dict['msg_calc_dvc_no'] = f"{str(lineno)}{str(trainno).zfill(3)}{str(nb67dict['dvc_carriage_no']).zfill(2)}"
        '''
        if dev_mode:
            nb67dict[
                'msg_calc_dvc_no'] = f"0{nb67dict['msg_line_no']}0{str(nb67dict['msg_train_no']).zfill(2)}0{nb67dict['msg_carriage_no']}"
            nb67dict[
                'msg_calc_train_no'] = f"0{nb67dict['msg_line_no']}0{str(nb67dict['msg_train_no']).zfill(2)}"
        else:
            nb67dict[
                'msg_calc_dvc_no'] = f"{str(nb67dict['msg_train_no']).zfill(5)}0{nb67dict['msg_carriage_no']}"
            nb67dict[
                'msg_calc_train_no'] = f"{str(nb67dict['msg_train_no']).zfill(5)}"
        '''
        nb67dict[
            'msg_calc_dvc_time'] = f"20{nb67dict['msg_src_dvc_year']}-{nb67dict['msg_src_dvc_month']}-{nb67dict['msg_src_dvc_day']} {nb67dict['msg_src_dvc_hour']}:{nb67dict['msg_src_dvc_minute']}:{nb67dict['msg_src_dvc_second']}"
        nb67dict['msg_calc_parse_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        keylst = list(nb67dict.keys()).copy()
        for k in keylst:
            if k.startswith('ig_rsv') or k.startswith('wrsv') or k.startswith('humdity') or k.__contains__('rsv_'):
                #log.debug(k)
                del nb67dict[k]
        for k in ('_io', '_parent', '_root',
                    'msg_reversed1', 'msg_reversed2', 'msg_reversed3', 'msg_reversed4', 'msg_reversed5',
                    'msg_src_dvc_year', 'msg_src_dvc_month', 'msg_src_dvc_day',
                    'msg_src_dvc_hour', 'msg_src_dvc_minute', 'msg_src_dvc_second'):
            if k in nb67dict:
                del nb67dict[k]
        for key,value in nb67dict.items():
            if isinstance(value, bool):
                if value:
                    nb67dict[key] = 1
                else:
                    nb67dict[key] = 0
            if key in div100list:
                if value/100 == value//100:
                    nb67dict[key] = value//100
                else:
                    nb67dict[key] = round(value/100,2)
            if key in div10list:
                if value/10 == value//10:
                    nb67dict[key] = value//10
                else:
                    nb67dict[key] = round(value/10,1)
        return nb67dict

    def from_bytes_to_dict(bytesobj):
        dev_mode = settings.DEV_MODE
        nb67dict = Nb67.from_bytes(bytesobj).__dict__.copy()
        lineno = int(str(nb67dict['dvc_train_no'])[:-3])
        trainno = int(str(nb67dict['dvc_train_no'])[-3:])
        nb67dict['msg_calc_train_no'] = f"{str(lineno)}{str(trainno).zfill(3)}"
        nb67dict['msg_calc_dvc_no'] = f"{str(lineno)}{str(trainno).zfill(3)}{str(nb67dict['dvc_carriage_no']).zfill(2)}"
        '''
        if dev_mode:
            nb67dict[
                'msg_calc_dvc_no'] = f"0{nb67dict['msg_line_no']}0{str(nb67dict['msg_train_no']).zfill(2)}0{nb67dict['msg_carriage_no']}"
            nb67dict[
                'msg_calc_train_no'] = f"0{nb67dict['msg_line_no']}0{str(nb67dict['msg_train_no']).zfill(2)}"
        else:
            nb67dict[
                'msg_calc_dvc_no'] = f"{str(nb67dict['msg_train_no']).zfill(5)}0{nb67dict['msg_carriage_no']}"
            nb67dict[
                'msg_calc_train_no'] = f"{str(nb67dict['msg_train_no']).zfill(5)}"
        '''
        nb67dict[
            'msg_calc_dvc_time'] = f"20{nb67dict['msg_src_dvc_year']}-{nb67dict['msg_src_dvc_month']}-{nb67dict['msg_src_dvc_day']} {nb67dict['msg_src_dvc_hour']}:{nb67dict['msg_src_dvc_minute']}:{nb67dict['msg_src_dvc_second']}"
        nb67dict['msg_calc_parse_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        keylst = list(nb67dict.keys()).copy()
        for k in keylst:
            if k.startswith('ig_rsv') or k.startswith('wrsv') or k.startswith('humdity') or k.__contains__('rsv_'):
                # log.debug(k)
                del nb67dict[k]
        for k in ('_io', '_parent', '_root',
                  'msg_reversed1', 'msg_reversed2', 'msg_reversed3', 'msg_reversed4', 'msg_reversed5',
                  'msg_src_dvc_year', 'msg_src_dvc_month', 'msg_src_dvc_day',
                  'msg_src_dvc_hour', 'msg_src_dvc_minute', 'msg_src_dvc_second'):
            if k in nb67dict:
                del nb67dict[k]
        for key,value in nb67dict.items():
            if isinstance(value, bool):
                if value:
                    nb67dict[key] = 1
                else:
                    nb67dict[key] = 0
            if key in div100list:
                if value / 100 == value // 100:
                    nb67dict[key] = value // 100
                else:
                    nb67dict[key] = round(value / 100, 2)
            if key in div10list:
                if value / 10 == value // 10:
                    nb67dict[key] = value // 10
                else:
                    nb67dict[key] = round(value / 10, 1)
        return nb67dict
