-- 创建视图：dev_mview_train_carriage
CREATE MATERIALIZED  VIEW dev_mview_train_carriage AS
SELECT DISTINCT
    dvc_train_no,
    dvc_carriage_no
FROM
    dev_macda
WHERE
    msg_calc_parse_time >= CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai' - INTERVAL '24 hours'
WITH NO DATA; -- 初始不加载数据，需要手动REFRESH
REFRESH MATERIALIZED VIEW dev_mview_train_carriage;

-- 创建视图：dev_mview_param
CREATE MATERIALIZED  VIEW dev_mview_param AS
SELECT
    msg_calc_dvc_time,
    msg_calc_parse_time,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    -- 动态筛选所有Param类型且存在于dev_macda的字段
    fas_sys,
    ras_sys,
    tic,
    "load",
    tveh_1,
    humdity_1,
    tveh_2,
    humdity_2,
    aq_t_u1,
    aq_h_u1,
    aq_co2_u1,
    aq_tvoc_u1,
    aq_pm2_5_u1,
    aq_pm10_u1,
    wmode_u1,
    presdiff_u1,
    fas_u1,
    ras_u1,
    fadpos_u1,
    radpos_u1,
    f_cp_u11,
    i_cp_u11,
    v_cp_u11,
    p_cp_u11,
    suckt_u11,
    suckp_u11,
    sp_u11,
    eevpos_u11,
    highpress_u11,
    sas_u11,
    f_cp_u12,
    i_cp_u12,
    v_cp_u12,
    p_cp_u12,
    suckt_u12,
    suckp_u12,
    sp_u12,
    eevpos_u12,
    highpress_u12,
    sas_u12,
    aq_t_u2,
    aq_h_u2,
    aq_co2_u2,
    aq_tvoc_u2,
    aq_pm2_5_u2,
    aq_pm10_u2,
    wmode_u2,
    presdiff_u2,
    fas_u2,
    ras_u2,
    fadpos_u2,
    radpos_u2,
    f_cp_u21,
    i_cp_u21,
    v_cp_u21,
    p_cp_u21,
    suckt_u21,
    suckp_u21,
    sp_u21,
    eevpos_u21,
    highpress_u21,
    sas_u21,
    f_cp_u22,
    i_cp_u22,
    v_cp_u22,
    p_cp_u22,
    suckt_u22,
    suckp_u22,
    sp_u22,
    eevpos_u22,
    highpress_u22,
    sas_u22,
    i_ef_u11,
    i_ef_u12,
    i_cf_u11,
    i_cf_u12,
    i_ef_u21,
    i_ef_u22,
    i_cf_u21,
    i_cf_u22,
    i_hvac_u1,
    i_hvac_u2,
    dwpower
FROM
    dev_macda
WITH NO DATA; -- 初始不加载数据，需要手动REFRESH
CREATE INDEX idx_dev_param_parse_time ON dev_mview_param(msg_calc_parse_time);
REFRESH MATERIALIZED VIEW dev_mview_param;

-- 创建视图：dev_mview_error
CREATE MATERIALIZED  VIEW dev_mview_error AS
SELECT
    msg_calc_dvc_time,
    msg_calc_parse_time,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    -- 动态筛选所有Error类型且存在于dev_macda的字段
    bocflt_ef_u11,
    bocflt_ef_u12,
    bocflt_cf_u11,
    bocflt_cf_u12,
    bflt_vfd_u11,
    bflt_vfd_com_u11,
    bflt_vfd_u12,
    bflt_vfd_com_u12,
    blpflt_comp_u11,
    bscflt_comp_u11,
    bscflt_vent_u11,
    blpflt_comp_u12,
    bscflt_comp_u12,
    bscflt_vent_u12,
    bflt_fad_u11,
    bflt_fad_u12,
    bflt_fad_u13,
    bflt_fad_u14,
    bflt_rad_u11,
    bflt_rad_u12,
    bflt_rad_u13,
    bflt_rad_u14,
    bflt_ap_u11,
    bflt_ap_u12,
    bflt_expboard_u1,
    bflt_frstemp_u1,
    bflt_rnttemp_u1,
    bflt_splytemp_u11,
    bflt_splytemp_u12,
    bflt_insptemp_u11,
    bflt_insptemp_u12,
    bflt_lowpres_u11,
    bflt_lowpres_u12,
    bflt_highpres_u11,
    bflt_highpres_u12,
    bflt_diffpres_u1,
    bocflt_ef_u21,
    bocflt_ef_u22,
    bocflt_cf_u21,
    bocflt_cf_u22,
    bflt_vfd_u21,
    bflt_vfd_com_u21,
    bflt_vfd_u22,
    bflt_vfd_com_u22,
    blpflt_comp_u21,
    bscflt_comp_u21,
    bscflt_vent_u21,
    blpflt_comp_u22,
    bscflt_comp_u22,
    bscflt_vent_u22,
    bflt_fad_u21,
    bflt_fad_u22,
    bflt_fad_u23,
    bflt_fad_u24,
    bflt_rad_u21,
    bflt_rad_u22,
    bflt_rad_u23,
    bflt_rad_u24,
    bflt_ap_u21,
    bflt_ap_u22,
    bflt_expboard_u2,
    bflt_frstemp_u2,
    bflt_rnttemp_u2,
    bflt_splytemp_u21,
    bflt_splytemp_u22,
    bflt_insptemp_u21,
    bflt_insptemp_u22,
    bflt_lowpres_u21,
    bflt_lowpres_u22,
    bflt_highpres_u21,
    bflt_highpres_u22,
    bflt_diffpres_u2,
    bflt_emergivt,
    bflt_vehtemp_u1,
    bflt_vehhum_u1,
    bflt_vehtemp_u2,
    bflt_vehhum_u2,
    bflt_airmon_u1,
    bflt_airmon_u2,
    bflt_currentmon,
    bflt_tcms,
    bscflt_ef_u1,
    bscflt_cf_u1,
    bscflt_vfd_pw_u11,
    bscflt_vfd_pw_u12,
    bscflt_ef_u2,
    bscflt_cf_u2,
    bscflt_vfd_pw_u21,
    bscflt_vfd_pw_u22,
    bflt_ef_cnt_u1,
    bflt_cf_cnt_u11,
    bflt_cf_cnt_u12,
    bflt_vfd_cnt_u11,
    bflt_vfd_cnt_u12,
    bflt_ev_cnt_u1,
    bflt_ef_cnt_u2,
    bflt_cf_cnt_u21,
    bflt_cf_cnt_u22,
    bflt_vfd_cnt_u21,
    bflt_vfd_cnt_u22,
    bflt_ev_cnt_u2,
    bflt_tempover
FROM
    dev_macda
WITH NO DATA; -- 初始不加载数据，需要手动REFRESH
CREATE INDEX idx_dev_error_parse_time ON dev_mview_error(msg_calc_parse_time);
REFRESH MATERIALIZED VIEW dev_mview_error;

-- 创建视图：dev_mview_statistic
CREATE MATERIALIZED  VIEW dev_mview_statistic AS
SELECT
    msg_calc_dvc_time,
    msg_calc_parse_time,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    -- 动态筛选所有Statistics类型且存在于dev_macda的字段
    dwemerg_op_tm,
    dwemerg_op_cnt,
    dwef_op_tm_u11,
    dwef_op_tm_u12,
    dwcf_op_tm_u11,
    dwcf_op_tm_u12,
    dwcp_op_tm_u11,
    dwcp_op_tm_u12,
    dwap_op_tm_u11,
    dwap_op_tm_u12,
    dwfad_op_cnt_u1,
    dwrad_op_cnt_u1,
    dwef_op_cnt_u11,
    dwcf_op_cnt_u11,
    dwcf_op_cnt_u12,
    dwcp_op_cnt_u11,
    dwcp_op_cnt_u12,
    dwap_op_cnt_u11,
    dwap_op_cnt_u12,
    dwef_op_tm_u21,
    dwcf_op_tm_u21,
    dwcf_op_tm_u22,
    dwcp_op_tm_u21,
    dwcp_op_tm_u22,
    dwap_op_tm_u21,
    dwap_op_tm_u22,
    dwfad_op_cnt_u2,
    dwrad_op_cnt_u2,
    dwef_op_cnt_u21,
    dwcf_op_cnt_u21,
    dwcf_op_cnt_u22,
    dwcp_op_cnt_u21,
    dwcp_op_cnt_u22,
    dwap_op_cnt_u21,
    dwap_op_cnt_u22
FROM
    dev_macda
WITH NO DATA; -- 初始不加载数据，需要手动REFRESH
CREATE INDEX idx_dev_statistic_parse_time ON dev_mview_statistic(msg_calc_parse_time);
REFRESH MATERIALIZED VIEW dev_mview_statistic;

-- 创建视图：dev_mview_param_transposed（参数类型字段转置）
CREATE MATERIALIZED  VIEW dev_mview_param_transposed AS
SELECT
    p.msg_calc_dvc_time,
    p.msg_calc_parse_time,
    p.msg_calc_dvc_no,
    p.msg_calc_train_no,
    p.dvc_train_no,
    p.dvc_carriage_no,
    f.field_name AS param_name,
    CASE
        WHEN f.field_code = 'fas_sys' THEN p.fas_sys
        WHEN f.field_code = 'ras_sys' THEN p.ras_sys
        WHEN f.field_code = 'tic' THEN p.tic
        WHEN f.field_code = 'load' THEN p."load"
        WHEN f.field_code = 'tveh_1' THEN p.tveh_1
        WHEN f.field_code = 'humdity_1' THEN p.humdity_1
        WHEN f.field_code = 'tveh_2' THEN p.tveh_2
        WHEN f.field_code = 'humdity_2' THEN p.humdity_2
        WHEN f.field_code = 'aq_t_u1' THEN p.aq_t_u1
        WHEN f.field_code = 'aq_h_u1' THEN p.aq_h_u1
        WHEN f.field_code = 'aq_co2_u1' THEN p.aq_co2_u1
        WHEN f.field_code = 'aq_tvoc_u1' THEN p.aq_tvoc_u1
        WHEN f.field_code = 'aq_pm2_5_u1' THEN p.aq_pm2_5_u1
        WHEN f.field_code = 'aq_pm10_u1' THEN p.aq_pm10_u1
        WHEN f.field_code = 'wmode_u1' THEN p.wmode_u1
        WHEN f.field_code = 'presdiff_u1' THEN p.presdiff_u1
        WHEN f.field_code = 'fas_u1' THEN p.fas_u1
        WHEN f.field_code = 'ras_u1' THEN p.ras_u1
        WHEN f.field_code = 'fadpos_u1' THEN p.fadpos_u1
        WHEN f.field_code = 'radpos_u1' THEN p.radpos_u1
        WHEN f.field_code = 'f_cp_u11' THEN p.f_cp_u11
        WHEN f.field_code = 'i_cp_u11' THEN p.i_cp_u11
        WHEN f.field_code = 'v_cp_u11' THEN p.v_cp_u11
        WHEN f.field_code = 'p_cp_u11' THEN p.p_cp_u11
        WHEN f.field_code = 'suckt_u11' THEN p.suckt_u11
        WHEN f.field_code = 'suckp_u11' THEN p.suckp_u11
        WHEN f.field_code = 'sp_u11' THEN p.sp_u11
        WHEN f.field_code = 'eevpos_u11' THEN p.eevpos_u11
        WHEN f.field_code = 'highpress_u11' THEN p.highpress_u11
        WHEN f.field_code = 'sas_u11' THEN p.sas_u11
        WHEN f.field_code = 'f_cp_u12' THEN p.f_cp_u12
        WHEN f.field_code = 'i_cp_u12' THEN p.i_cp_u12
        WHEN f.field_code = 'v_cp_u12' THEN p.v_cp_u12
        WHEN f.field_code = 'p_cp_u12' THEN p.p_cp_u12
        WHEN f.field_code = 'suckt_u12' THEN p.suckt_u12
        WHEN f.field_code = 'suckp_u12' THEN p.suckp_u12
        WHEN f.field_code = 'sp_u12' THEN p.sp_u12
        WHEN f.field_code = 'eevpos_u12' THEN p.eevpos_u12
        WHEN f.field_code = 'highpress_u12' THEN p.highpress_u12
        WHEN f.field_code = 'sas_u12' THEN p.sas_u12
        WHEN f.field_code = 'aq_t_u2' THEN p.aq_t_u2
        WHEN f.field_code = 'aq_h_u2' THEN p.aq_h_u2
        WHEN f.field_code = 'aq_co2_u2' THEN p.aq_co2_u2
        WHEN f.field_code = 'aq_tvoc_u2' THEN p.aq_tvoc_u2
        WHEN f.field_code = 'aq_pm2_5_u2' THEN p.aq_pm2_5_u2
        WHEN f.field_code = 'aq_pm10_u2' THEN p.aq_pm10_u2
        WHEN f.field_code = 'wmode_u2' THEN p.wmode_u2
        WHEN f.field_code = 'presdiff_u2' THEN p.presdiff_u2
        WHEN f.field_code = 'fas_u2' THEN p.fas_u2
        WHEN f.field_code = 'ras_u2' THEN p.ras_u2
        WHEN f.field_code = 'fadpos_u2' THEN p.fadpos_u2
        WHEN f.field_code = 'radpos_u2' THEN p.radpos_u2
        WHEN f.field_code = 'f_cp_u21' THEN p.f_cp_u21
        WHEN f.field_code = 'i_cp_u21' THEN p.i_cp_u21
        WHEN f.field_code = 'v_cp_u21' THEN p.v_cp_u21
        WHEN f.field_code = 'p_cp_u21' THEN p.p_cp_u21
        WHEN f.field_code = 'suckt_u21' THEN p.suckt_u21
        WHEN f.field_code = 'suckp_u21' THEN p.suckp_u21
        WHEN f.field_code = 'sp_u21' THEN p.sp_u21
        WHEN f.field_code = 'eevpos_u21' THEN p.eevpos_u21
        WHEN f.field_code = 'highpress_u21' THEN p.highpress_u21
        WHEN f.field_code = 'sas_u21' THEN p.sas_u21
        WHEN f.field_code = 'f_cp_u22' THEN p.f_cp_u22
        WHEN f.field_code = 'i_cp_u22' THEN p.i_cp_u22
        WHEN f.field_code = 'v_cp_u22' THEN p.v_cp_u22
        WHEN f.field_code = 'p_cp_u22' THEN p.p_cp_u22
        WHEN f.field_code = 'suckt_u22' THEN p.suckt_u22
        WHEN f.field_code = 'suckp_u22' THEN p.suckp_u22
        WHEN f.field_code = 'sp_u22' THEN p.sp_u22
        WHEN f.field_code = 'eevpos_u22' THEN p.eevpos_u22
        WHEN f.field_code = 'highpress_u22' THEN p.highpress_u22
        WHEN f.field_code = 'sas_u22' THEN p.sas_u22
        WHEN f.field_code = 'i_ef_u11' THEN p.i_ef_u11
        WHEN f.field_code = 'i_ef_u12' THEN p.i_ef_u12
        WHEN f.field_code = 'i_cf_u11' THEN p.i_cf_u11
        WHEN f.field_code = 'i_cf_u12' THEN p.i_cf_u12
        WHEN f.field_code = 'i_ef_u21' THEN p.i_ef_u21
        WHEN f.field_code = 'i_ef_u22' THEN p.i_ef_u22
        WHEN f.field_code = 'i_cf_u21' THEN p.i_cf_u21
        WHEN f.field_code = 'i_cf_u22' THEN p.i_cf_u22
        WHEN f.field_code = 'i_hvac_u1' THEN p.i_hvac_u1
        WHEN f.field_code = 'i_hvac_u2' THEN p.i_hvac_u2
        WHEN f.field_code = 'dwpower' THEN p.dwpower
        ELSE NULL
    END AS param_value
FROM
    dev_mview_param p
CROSS JOIN
    sys_fields f
WHERE
    f.field_code IN (
        'fas_sys', 'ras_sys', 'tic', 'load', 'tveh_1', 'humdity_1', 'tveh_2', 'humdity_2',
        'aq_t_u1', 'aq_h_u1', 'aq_co2_u1', 'aq_tvoc_u1', 'aq_pm2_5_u1', 'aq_pm10_u1',
        'wmode_u1', 'presdiff_u1', 'fas_u1', 'ras_u1', 'fadpos_u1', 'radpos_u1',
        'f_cp_u11', 'i_cp_u11', 'v_cp_u11', 'p_cp_u11', 'suckt_u11', 'suckp_u11',
        'sp_u11', 'eevpos_u11', 'highpress_u11', 'sas_u11', 'f_cp_u12', 'i_cp_u12',
        'v_cp_u12', 'p_cp_u12', 'suckt_u12', 'suckp_u12', 'sp_u12', 'eevpos_u12',
        'highpress_u12', 'sas_u12', 'aq_t_u2', 'aq_h_u2', 'aq_co2_u2', 'aq_tvoc_u2',
        'aq_pm2_5_u2', 'aq_pm10_u2', 'wmode_u2', 'presdiff_u2', 'fas_u2', 'ras_u2',
        'fadpos_u2', 'radpos_u2', 'f_cp_u21', 'i_cp_u21', 'v_cp_u21', 'p_cp_u21',
        'suckt_u21', 'suckp_u21', 'sp_u21', 'eevpos_u21', 'highpress_u21', 'sas_u21',
        'f_cp_u22', 'i_cp_u22', 'v_cp_u22', 'p_cp_u22', 'suckt_u22', 'suckp_u22',
        'sp_u22', 'eevpos_u22', 'highpress_u22', 'sas_u22', 'i_ef_u11', 'i_ef_u12',
        'i_cf_u11', 'i_cf_u12', 'i_ef_u21', 'i_ef_u22', 'i_cf_u21', 'i_cf_u22',
        'i_hvac_u1', 'i_hvac_u2', 'dwpower'
    )
ORDER BY
    f.field_name,
    p.msg_calc_parse_time
WITH NO DATA; -- 初始不加载数据，需要手动REFRESH
CREATE INDEX idx_dev_param_transposed_param_name ON dev_mview_param_transposed(param_name);
CREATE INDEX idx_dev_param_transposed_parse_time ON dev_mview_param_transposed(msg_calc_parse_time);
CREATE INDEX idx_dev_param_transposed_param_value ON dev_mview_param_transposed(param_value);
REFRESH MATERIALIZED VIEW dev_mview_param_transposed;


-- 创建视图：dev_mview_error_transposed（错误类型字段转置）
CREATE MATERIALIZED  VIEW dev_mview_error_transposed AS
SELECT
    e.msg_calc_dvc_time,
    e.msg_calc_parse_time,
    e.msg_calc_dvc_no,
    e.msg_calc_train_no,
    e.dvc_train_no,
    e.dvc_carriage_no,
    f.field_name AS param_name,
    CASE
        WHEN f.field_code = 'bocflt_ef_u11' THEN e.bocflt_ef_u11
        WHEN f.field_code = 'bocflt_ef_u12' THEN e.bocflt_ef_u12
        WHEN f.field_code = 'bocflt_cf_u11' THEN e.bocflt_cf_u11
        WHEN f.field_code = 'bocflt_cf_u12' THEN e.bocflt_cf_u12
        WHEN f.field_code = 'bflt_vfd_u11' THEN e.bflt_vfd_u11
        WHEN f.field_code = 'bflt_vfd_com_u11' THEN e.bflt_vfd_com_u11
        WHEN f.field_code = 'bflt_vfd_u12' THEN e.bflt_vfd_u12
        WHEN f.field_code = 'bflt_vfd_com_u12' THEN e.bflt_vfd_com_u12
        WHEN f.field_code = 'blpflt_comp_u11' THEN e.blpflt_comp_u11
        WHEN f.field_code = 'bscflt_comp_u11' THEN e.bscflt_comp_u11
        WHEN f.field_code = 'bscflt_vent_u11' THEN e.bscflt_vent_u11
        WHEN f.field_code = 'blpflt_comp_u12' THEN e.blpflt_comp_u12
        WHEN f.field_code = 'bscflt_comp_u12' THEN e.bscflt_comp_u12
        WHEN f.field_code = 'bscflt_vent_u12' THEN e.bscflt_vent_u12
        WHEN f.field_code = 'bflt_fad_u11' THEN e.bflt_fad_u11
        WHEN f.field_code = 'bflt_fad_u12' THEN e.bflt_fad_u12
        WHEN f.field_code = 'bflt_fad_u13' THEN e.bflt_fad_u13
        WHEN f.field_code = 'bflt_fad_u14' THEN e.bflt_fad_u14
        WHEN f.field_code = 'bflt_rad_u11' THEN e.bflt_rad_u11
        WHEN f.field_code = 'bflt_rad_u12' THEN e.bflt_rad_u12
        WHEN f.field_code = 'bflt_rad_u13' THEN e.bflt_rad_u13
        WHEN f.field_code = 'bflt_rad_u14' THEN e.bflt_rad_u14
        WHEN f.field_code = 'bflt_ap_u11' THEN e.bflt_ap_u11
        WHEN f.field_code = 'bflt_ap_u12' THEN e.bflt_ap_u12
        WHEN f.field_code = 'bflt_expboard_u1' THEN e.bflt_expboard_u1
        WHEN f.field_code = 'bflt_frstemp_u1' THEN e.bflt_frstemp_u1
        WHEN f.field_code = 'bflt_rnttemp_u1' THEN e.bflt_rnttemp_u1
        WHEN f.field_code = 'bflt_splytemp_u11' THEN e.bflt_splytemp_u11
        WHEN f.field_code = 'bflt_splytemp_u12' THEN e.bflt_splytemp_u12
        WHEN f.field_code = 'bflt_insptemp_u11' THEN e.bflt_insptemp_u11
        WHEN f.field_code = 'bflt_insptemp_u12' THEN e.bflt_insptemp_u12
        WHEN f.field_code = 'bflt_lowpres_u11' THEN e.bflt_lowpres_u11
        WHEN f.field_code = 'bflt_lowpres_u12' THEN e.bflt_lowpres_u12
        WHEN f.field_code = 'bflt_highpres_u11' THEN e.bflt_highpres_u11
        WHEN f.field_code = 'bflt_highpres_u12' THEN e.bflt_highpres_u12
        WHEN f.field_code = 'bflt_diffpres_u1' THEN e.bflt_diffpres_u1
        WHEN f.field_code = 'bocflt_ef_u21' THEN e.bocflt_ef_u21
        WHEN f.field_code = 'bocflt_ef_u22' THEN e.bocflt_ef_u22
        WHEN f.field_code = 'bocflt_cf_u21' THEN e.bocflt_cf_u21
        WHEN f.field_code = 'bocflt_cf_u22' THEN e.bocflt_cf_u22
        WHEN f.field_code = 'bflt_vfd_u21' THEN e.bflt_vfd_u21
        WHEN f.field_code = 'bflt_vfd_com_u21' THEN e.bflt_vfd_com_u21
        WHEN f.field_code = 'bflt_vfd_u22' THEN e.bflt_vfd_u22
        WHEN f.field_code = 'bflt_vfd_com_u22' THEN e.bflt_vfd_com_u22
        WHEN f.field_code = 'blpflt_comp_u21' THEN e.blpflt_comp_u21
        WHEN f.field_code = 'bscflt_comp_u21' THEN e.bscflt_comp_u21
        WHEN f.field_code = 'bscflt_vent_u21' THEN e.bscflt_vent_u21
        WHEN f.field_code = 'blpflt_comp_u22' THEN e.blpflt_comp_u22
        WHEN f.field_code = 'bscflt_comp_u22' THEN e.bscflt_comp_u22
        WHEN f.field_code = 'bscflt_vent_u22' THEN e.bscflt_vent_u22
        WHEN f.field_code = 'bflt_fad_u21' THEN e.bflt_fad_u21
        WHEN f.field_code = 'bflt_fad_u22' THEN e.bflt_fad_u22
        WHEN f.field_code = 'bflt_fad_u23' THEN e.bflt_fad_u23
        WHEN f.field_code = 'bflt_fad_u24' THEN e.bflt_fad_u24
        WHEN f.field_code = 'bflt_rad_u21' THEN e.bflt_rad_u21
        WHEN f.field_code = 'bflt_rad_u22' THEN e.bflt_rad_u22
        WHEN f.field_code = 'bflt_rad_u23' THEN e.bflt_rad_u23
        WHEN f.field_code = 'bflt_rad_u24' THEN e.bflt_rad_u24
        WHEN f.field_code = 'bflt_ap_u21' THEN e.bflt_ap_u21
        WHEN f.field_code = 'bflt_ap_u22' THEN e.bflt_ap_u22
        WHEN f.field_code = 'bflt_expboard_u2' THEN e.bflt_expboard_u2
        WHEN f.field_code = 'bflt_frstemp_u2' THEN e.bflt_frstemp_u2
        WHEN f.field_code = 'bflt_rnttemp_u2' THEN e.bflt_rnttemp_u2
        WHEN f.field_code = 'bflt_splytemp_u21' THEN e.bflt_splytemp_u21
        WHEN f.field_code = 'bflt_splytemp_u22' THEN e.bflt_splytemp_u22
        WHEN f.field_code = 'bflt_insptemp_u21' THEN e.bflt_insptemp_u21
        WHEN f.field_code = 'bflt_insptemp_u22' THEN e.bflt_insptemp_u22
        WHEN f.field_code = 'bflt_lowpres_u21' THEN e.bflt_lowpres_u21
        WHEN f.field_code = 'bflt_lowpres_u22' THEN e.bflt_lowpres_u22
        WHEN f.field_code = 'bflt_highpres_u21' THEN e.bflt_highpres_u21
        WHEN f.field_code = 'bflt_highpres_u22' THEN e.bflt_highpres_u22
        WHEN f.field_code = 'bflt_diffpres_u2' THEN e.bflt_diffpres_u2
        WHEN f.field_code = 'bflt_emergivt' THEN e.bflt_emergivt
        WHEN f.field_code = 'bflt_vehtemp_u1' THEN e.bflt_vehtemp_u1
        WHEN f.field_code = 'bflt_vehhum_u1' THEN e.bflt_vehhum_u1
        WHEN f.field_code = 'bflt_vehtemp_u2' THEN e.bflt_vehtemp_u2
        WHEN f.field_code = 'bflt_vehhum_u2' THEN e.bflt_vehhum_u2
        WHEN f.field_code = 'bflt_airmon_u1' THEN e.bflt_airmon_u1
        WHEN f.field_code = 'bflt_airmon_u2' THEN e.bflt_airmon_u2
        WHEN f.field_code = 'bflt_currentmon' THEN e.bflt_currentmon
        WHEN f.field_code = 'bflt_tcms' THEN e.bflt_tcms
        WHEN f.field_code = 'bscflt_ef_u1' THEN e.bscflt_ef_u1
        WHEN f.field_code = 'bscflt_cf_u1' THEN e.bscflt_cf_u1
        WHEN f.field_code = 'bscflt_vfd_pw_u11' THEN e.bscflt_vfd_pw_u11
        WHEN f.field_code = 'bscflt_vfd_pw_u12' THEN e.bscflt_vfd_pw_u12
        WHEN f.field_code = 'bscflt_ef_u2' THEN e.bscflt_ef_u2
        WHEN f.field_code = 'bscflt_cf_u2' THEN e.bscflt_cf_u2
        WHEN f.field_code = 'bscflt_vfd_pw_u21' THEN e.bscflt_vfd_pw_u21
        WHEN f.field_code = 'bscflt_vfd_pw_u22' THEN e.bscflt_vfd_pw_u22
        WHEN f.field_code = 'bflt_ef_cnt_u1' THEN e.bflt_ef_cnt_u1
        WHEN f.field_code = 'bflt_cf_cnt_u11' THEN e.bflt_cf_cnt_u11
        WHEN f.field_code = 'bflt_cf_cnt_u12' THEN e.bflt_cf_cnt_u12
        WHEN f.field_code = 'bflt_vfd_cnt_u11' THEN e.bflt_vfd_cnt_u11
        WHEN f.field_code = 'bflt_vfd_cnt_u12' THEN e.bflt_vfd_cnt_u12
        WHEN f.field_code = 'bflt_ev_cnt_u1' THEN e.bflt_ev_cnt_u1
        WHEN f.field_code = 'bflt_ef_cnt_u2' THEN e.bflt_ef_cnt_u2
        WHEN f.field_code = 'bflt_cf_cnt_u21' THEN e.bflt_cf_cnt_u21
        WHEN f.field_code = 'bflt_cf_cnt_u22' THEN e.bflt_cf_cnt_u22
        WHEN f.field_code = 'bflt_vfd_cnt_u21' THEN e.bflt_vfd_cnt_u21
        WHEN f.field_code = 'bflt_vfd_cnt_u22' THEN e.bflt_vfd_cnt_u22
        WHEN f.field_code = 'bflt_ev_cnt_u2' THEN e.bflt_ev_cnt_u2
        WHEN f.field_code = 'bflt_tempover' THEN e.bflt_tempover
        ELSE NULL
    END AS param_value
FROM
    dev_mview_error e
CROSS JOIN
    sys_fields f
WHERE
    f.field_code IN (
        'bocflt_ef_u11', 'bocflt_ef_u12', 'bocflt_cf_u11', 'bocflt_cf_u12',
        'bflt_vfd_u11', 'bflt_vfd_com_u11', 'bflt_vfd_u12', 'bflt_vfd_com_u12',
        'blpflt_comp_u11', 'bscflt_comp_u11', 'bscflt_vent_u11', 'blpflt_comp_u12',
        'bscflt_comp_u12', 'bscflt_vent_u12', 'bflt_fad_u11', 'bflt_fad_u12',
        'bflt_fad_u13', 'bflt_fad_u14', 'bflt_rad_u11', 'bflt_rad_u12',
        'bflt_rad_u13', 'bflt_rad_u14', 'bflt_ap_u11', 'bflt_ap_u12',
        'bflt_expboard_u1', 'bflt_frstemp_u1', 'bflt_rnttemp_u1', 'bflt_splytemp_u11',
        'bflt_splytemp_u12', 'bflt_insptemp_u11', 'bflt_insptemp_u12', 'bflt_lowpres_u11',
        'bflt_lowpres_u12', 'bflt_highpres_u11', 'bflt_highpres_u12', 'bflt_diffpres_u1',
        'bocflt_ef_u21', 'bocflt_ef_u22', 'bocflt_cf_u21', 'bocflt_cf_u22',
        'bflt_vfd_u21', 'bflt_vfd_com_u21', 'bflt_vfd_u22', 'bflt_vfd_com_u22',
        'blpflt_comp_u21', 'bscflt_comp_u21', 'bscflt_vent_u21', 'blpflt_comp_u22',
        'bscflt_comp_u22', 'bscflt_vent_u22', 'bflt_fad_u21', 'bflt_fad_u22',
        'bflt_fad_u23', 'bflt_fad_u24', 'bflt_rad_u21', 'bflt_rad_u22',
        'bflt_rad_u23', 'bflt_rad_u24', 'bflt_ap_u21', 'bflt_ap_u22',
        'bflt_expboard_u2', 'bflt_frstemp_u2', 'bflt_rnttemp_u2', 'bflt_splytemp_u21',
        'bflt_splytemp_u22', 'bflt_insptemp_u21', 'bflt_insptemp_u22', 'bflt_lowpres_u21',
        'bflt_lowpres_u22', 'bflt_highpres_u21', 'bflt_highpres_u22', 'bflt_diffpres_u2',
        'bflt_emergivt', 'bflt_vehtemp_u1', 'bflt_vehhum_u1', 'bflt_vehtemp_u2',
        'bflt_vehhum_u2', 'bflt_airmon_u1', 'bflt_airmon_u2', 'bflt_currentmon',
        'bflt_tcms', 'bscflt_ef_u1', 'bscflt_cf_u1', 'bscflt_vfd_pw_u11',
        'bscflt_vfd_pw_u12', 'bscflt_ef_u2', 'bscflt_cf_u2', 'bscflt_vfd_pw_u21',
        'bscflt_vfd_pw_u22', 'bflt_ef_cnt_u1', 'bflt_cf_cnt_u11', 'bflt_cf_cnt_u12',
        'bflt_vfd_cnt_u11', 'bflt_vfd_cnt_u12', 'bflt_ev_cnt_u1', 'bflt_ef_cnt_u2',
        'bflt_cf_cnt_u21', 'bflt_cf_cnt_u22', 'bflt_vfd_cnt_u21', 'bflt_vfd_cnt_u22',
        'bflt_ev_cnt_u2', 'bflt_tempover'
    )
ORDER BY
    f.field_name,
    e.msg_calc_parse_time
WITH NO DATA; -- 初始不加载数据，需要手动REFRESH
CREATE INDEX idx_dev_error_transposed_param_name ON dev_mview_error_transposed(param_name);
CREATE INDEX idx_dev_error_transposed_parse_time ON dev_mview_error_transposed(msg_calc_parse_time);
CREATE INDEX idx_dev_error_transposed_param_value ON dev_mview_error_transposed(param_value);
CREATE INDEX idx_dev_mview_error_transposed_covering ON dev_mview_error_transposed (
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no
) INCLUDE (param_name, msg_calc_parse_time, param_value);
CREATE UNIQUE INDEX idx_mview_error_transposed_unique ON dev_mview_error_transposed (msg_calc_parse_time,dvc_train_no,dvc_carriage_no,param_name);
REFRESH MATERIALIZED VIEW dev_mview_error_transposed;

-- 创建视图：dev_mview_statistic_transposed（统计类型字段转置）
CREATE MATERIALIZED  VIEW dev_mview_statistic_transposed AS
SELECT
    s.msg_calc_dvc_time,
    s.msg_calc_parse_time,
    s.msg_calc_dvc_no,
    s.msg_calc_train_no,
    s.dvc_train_no,
    s.dvc_carriage_no,
    f.field_name AS param_name,
    CASE
        WHEN f.field_code = 'dwemerg_op_tm' THEN s.dwemerg_op_tm
        WHEN f.field_code = 'dwemerg_op_cnt' THEN s.dwemerg_op_cnt
        WHEN f.field_code = 'dwef_op_tm_u11' THEN s.dwef_op_tm_u11
        WHEN f.field_code = 'dwef_op_tm_u12' THEN s.dwef_op_tm_u12
        WHEN f.field_code = 'dwcf_op_tm_u11' THEN s.dwcf_op_tm_u11
        WHEN f.field_code = 'dwcf_op_tm_u12' THEN s.dwcf_op_tm_u12
        WHEN f.field_code = 'dwcp_op_tm_u11' THEN s.dwcp_op_tm_u11
        WHEN f.field_code = 'dwcp_op_tm_u12' THEN s.dwcp_op_tm_u12
        WHEN f.field_code = 'dwap_op_tm_u11' THEN s.dwap_op_tm_u11
        WHEN f.field_code = 'dwap_op_tm_u12' THEN s.dwap_op_tm_u12
        WHEN f.field_code = 'dwfad_op_cnt_u1' THEN s.dwfad_op_cnt_u1
        WHEN f.field_code = 'dwrad_op_cnt_u1' THEN s.dwrad_op_cnt_u1
        WHEN f.field_code = 'dwef_op_cnt_u11' THEN s.dwef_op_cnt_u11
        WHEN f.field_code = 'dwcf_op_cnt_u11' THEN s.dwcf_op_cnt_u11
        WHEN f.field_code = 'dwcf_op_cnt_u12' THEN s.dwcf_op_cnt_u12
        WHEN f.field_code = 'dwcp_op_cnt_u11' THEN s.dwcp_op_cnt_u11
        WHEN f.field_code = 'dwcp_op_cnt_u12' THEN s.dwcp_op_cnt_u12
        WHEN f.field_code = 'dwap_op_cnt_u11' THEN s.dwap_op_cnt_u11
        WHEN f.field_code = 'dwap_op_cnt_u12' THEN s.dwap_op_cnt_u12
        WHEN f.field_code = 'dwef_op_tm_u21' THEN s.dwef_op_tm_u21
        WHEN f.field_code = 'dwcf_op_tm_u21' THEN s.dwcf_op_tm_u21
        WHEN f.field_code = 'dwcf_op_tm_u22' THEN s.dwcf_op_tm_u22
        WHEN f.field_code = 'dwcp_op_tm_u21' THEN s.dwcp_op_tm_u21
        WHEN f.field_code = 'dwcp_op_tm_u22' THEN s.dwcp_op_tm_u22
        WHEN f.field_code = 'dwap_op_tm_u21' THEN s.dwap_op_tm_u21
        WHEN f.field_code = 'dwap_op_tm_u22' THEN s.dwap_op_tm_u22
        WHEN f.field_code = 'dwfad_op_cnt_u2' THEN s.dwfad_op_cnt_u2
        WHEN f.field_code = 'dwrad_op_cnt_u2' THEN s.dwrad_op_cnt_u2
        WHEN f.field_code = 'dwef_op_cnt_u21' THEN s.dwef_op_cnt_u21
        WHEN f.field_code = 'dwcf_op_cnt_u21' THEN s.dwcf_op_cnt_u21
        WHEN f.field_code = 'dwcf_op_cnt_u22' THEN s.dwcf_op_cnt_u22
        WHEN f.field_code = 'dwcp_op_cnt_u21' THEN s.dwcp_op_cnt_u21
        WHEN f.field_code = 'dwcp_op_cnt_u22' THEN s.dwcp_op_cnt_u22
        WHEN f.field_code = 'dwap_op_cnt_u21' THEN s.dwap_op_cnt_u21
        WHEN f.field_code = 'dwap_op_cnt_u22' THEN s.dwap_op_cnt_u22
        ELSE NULL
    END AS param_value
FROM
    dev_mview_statistic s
CROSS JOIN
    sys_fields f
WHERE
    f.field_code IN (
        'dwemerg_op_tm', 'dwemerg_op_cnt', 'dwef_op_tm_u11', 'dwef_op_tm_u12',
        'dwcf_op_tm_u11', 'dwcf_op_tm_u12', 'dwcp_op_tm_u11', 'dwcp_op_tm_u12',
        'dwap_op_tm_u11', 'dwap_op_tm_u12', 'dwfad_op_cnt_u1', 'dwrad_op_cnt_u1',
        'dwef_op_cnt_u11', 'dwcf_op_cnt_u11', 'dwcf_op_cnt_u12', 'dwcp_op_cnt_u11',
        'dwcp_op_cnt_u12', 'dwap_op_cnt_u11', 'dwap_op_cnt_u12', 'dwef_op_tm_u21',
        'dwcf_op_tm_u21', 'dwcf_op_tm_u22', 'dwcp_op_tm_u21', 'dwcp_op_tm_u22',
        'dwap_op_tm_u21', 'dwap_op_tm_u22', 'dwfad_op_cnt_u2', 'dwrad_op_cnt_u2',
        'dwef_op_cnt_u21', 'dwcf_op_cnt_u21', 'dwcf_op_cnt_u22', 'dwcp_op_cnt_u21',
        'dwcp_op_cnt_u22', 'dwap_op_cnt_u21', 'dwap_op_cnt_u22'
    )
ORDER BY
    f.field_name,
    s.msg_calc_parse_time
WITH NO DATA; -- 初始不加载数据，需要手动REFRESH
CREATE INDEX idx_dev_statistic_transposed_param_name ON dev_mview_statistic_transposed(param_name);
CREATE INDEX idx_dev_statistic_transposed_parse_time ON dev_mview_statistic_transposed(msg_calc_parse_time);
CREATE INDEX idx_dev_statistic_transposed_param_value ON dev_mview_statistic_transposed(param_value);
REFRESH MATERIALIZED VIEW dev_mview_statistic_transposed;


-- 在物化视图中添加`last_refresh_time`字段
ALTER TABLE dev_mview_error_transposed ADD COLUMN last_refresh_time TIMESTAMP DEFAULT NOW();

-- 增量刷新函数
CREATE OR REPLACE FUNCTION refresh_incrementally()
RETURNS void AS $$
BEGIN
    UPDATE dev_mview_error_transposed
    SET last_refresh_time = NOW()
    WHERE msg_calc_parse_time = (SELECT MAX(msg_calc_parse_time) FROM dev_view_error);

    REFRESH MATERIALIZED VIEW CONCURRENTLY dev_mview_error_transposed
    WHERE msg_calc_parse_time > (SELECT last_refresh_time FROM dev_mview_error_transposed);
END;
$$ LANGUAGE plpgsql;

REFRESH MATERIALIZED VIEW CONCURRENTLY dev_mview_error_transposed;



