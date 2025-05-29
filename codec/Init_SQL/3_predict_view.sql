-- 创建视图：dev_view_predict
CREATE OR REPLACE VIEW dev_view_predict AS
SELECT
    -- 保留原表所有字段
    msg_calc_dvc_time,
    msg_calc_parse_time,
    msg_calc_dvc_no,
    msg_calc_train_no,
    -- 新增字段：车辆编号（整数类型）
    msg_calc_train_no::INTEGER AS dvc_train_no,
    -- 新增字段：车厢编号（通过截取设备编号的后缀部分并转为整数）
    SUBSTRING(msg_calc_dvc_no FROM LENGTH(msg_calc_train_no) + 1)::INTEGER AS dvc_carriage_no,
    ref_leak_u11,
    ref_leak_u12,
    ref_leak_u21,
    ref_leak_u22,
    f_cp_u1,
    f_cp_u2,
    f_fas,
    f_ras,
    cabin_overtemp,
    f_presdiff_u1,
    f_presdiff_u2,
    f_ef_u11,
    f_ef_u12,
    f_ef_u21,
    f_ef_u22,
    f_cf_u11,
    f_cf_u12,
    f_cf_u21,
    f_cf_u22,
    f_fas_u11,
    f_fas_u12,
    f_fas_u21,
    f_fas_u22,
    f_aq_u1,
    f_aq_u2
FROM dev_predict;

-- 创建视图：pro_view_predict
CREATE OR REPLACE VIEW pro_view_predict AS
SELECT
    -- 保留原表所有字段
    msg_calc_dvc_time,
    msg_calc_parse_time,
    msg_calc_dvc_no,
    msg_calc_train_no,
    -- 新增字段：车辆编号（整数类型）
    msg_calc_train_no::INTEGER AS dvc_train_no,
    -- 新增字段：车厢编号（通过截取设备编号的后缀部分并转为整数）
    SUBSTRING(msg_calc_dvc_no FROM LENGTH(msg_calc_train_no) + 1)::INTEGER AS dvc_carriage_no,
    ref_leak_u11,
    ref_leak_u12,
    ref_leak_u21,
    ref_leak_u22,
    f_cp_u1,
    f_cp_u2,
    f_fas,
    f_ras,
    cabin_overtemp,
    f_presdiff_u1,
    f_presdiff_u2,
    f_ef_u11,
    f_ef_u12,
    f_ef_u21,
    f_ef_u22,
    f_cf_u11,
    f_cf_u12,
    f_cf_u21,
    f_cf_u22,
    f_fas_u11,
    f_fas_u12,
    f_fas_u21,
    f_fas_u22,
    f_aq_u1,
    f_aq_u2
FROM pro_predict;

-- 创建视图：dev_view_predict_transposed
CREATE OR REPLACE VIEW dev_view_predict_transposed AS
SELECT
    dv.msg_calc_dvc_time,
    dv.msg_calc_parse_time,
    dv.msg_calc_dvc_no,
    dv.msg_calc_train_no,
    dv.dvc_train_no,
    dv.dvc_carriage_no,
    sf.field_name,
    val.field_value  -- 移除不必要的类型转换（原字段已是integer类型）
FROM dev_view_predict dv
-- 使用LATERAL展开时直接引用字段（避免重复表别名）
CROSS JOIN LATERAL (
    VALUES
        ('ref_leak_u11', ref_leak_u11),
        ('ref_leak_u12', ref_leak_u12),
        ('ref_leak_u21', ref_leak_u21),
        ('ref_leak_u22', ref_leak_u22),
        ('f_cp_u1', f_cp_u1),
        ('f_cp_u2', f_cp_u2),
        ('f_fas', f_fas),
        ('f_ras', f_ras),
        ('cabin_overtemp', cabin_overtemp),
        ('f_presdiff_u1', f_presdiff_u1),
        ('f_presdiff_u2', f_presdiff_u2),
        ('f_ef_u11', f_ef_u11),
        ('f_ef_u12', f_ef_u12),
        ('f_ef_u21', f_ef_u21),
        ('f_ef_u22', f_ef_u22),
        ('f_cf_u11', f_cf_u11),
        ('f_cf_u12', f_cf_u12),
        ('f_cf_u21', f_cf_u21),
        ('f_cf_u22', f_cf_u22),
        ('f_fas_u11', f_fas_u11),
        ('f_fas_u12', f_fas_u12),
        ('f_fas_u21', f_fas_u21),
        ('f_fas_u22', f_fas_u22),
        ('f_aq_u1', f_aq_u1),
        ('f_aq_u2', f_aq_u2)
) AS val(field_code, field_value)
-- 关联时使用索引加速
JOIN sys_fields sf
    ON val.field_code = sf.field_code
-- 仅保留必要排序（实际查询时建议在业务层排序）
ORDER BY sf.field_name, dv.msg_calc_parse_time;

-- 创建视图：pro_view_predict_transposed
CREATE OR REPLACE VIEW pro_view_predict_transposed AS
SELECT
    dv.msg_calc_dvc_time,
    dv.msg_calc_parse_time,
    dv.msg_calc_dvc_no,
    dv.msg_calc_train_no,
    dv.dvc_train_no,
    dv.dvc_carriage_no,
    sf.field_name,
    val.field_value  -- 移除不必要的类型转换（原字段已是integer类型）
FROM pro_view_predict dv
-- 使用LATERAL展开时直接引用字段（避免重复表别名）
CROSS JOIN LATERAL (
    VALUES
        ('ref_leak_u11', ref_leak_u11),
        ('ref_leak_u12', ref_leak_u12),
        ('ref_leak_u21', ref_leak_u21),
        ('ref_leak_u22', ref_leak_u22),
        ('f_cp_u1', f_cp_u1),
        ('f_cp_u2', f_cp_u2),
        ('f_fas', f_fas),
        ('f_ras', f_ras),
        ('cabin_overtemp', cabin_overtemp),
        ('f_presdiff_u1', f_presdiff_u1),
        ('f_presdiff_u2', f_presdiff_u2),
        ('f_ef_u11', f_ef_u11),
        ('f_ef_u12', f_ef_u12),
        ('f_ef_u21', f_ef_u21),
        ('f_ef_u22', f_ef_u22),
        ('f_cf_u11', f_cf_u11),
        ('f_cf_u12', f_cf_u12),
        ('f_cf_u21', f_cf_u21),
        ('f_cf_u22', f_cf_u22),
        ('f_fas_u11', f_fas_u11),
        ('f_fas_u12', f_fas_u12),
        ('f_fas_u21', f_fas_u21),
        ('f_fas_u22', f_fas_u22),
        ('f_aq_u1', f_aq_u1),
        ('f_aq_u2', f_aq_u2)
) AS val(field_code, field_value)
-- 关联时使用索引加速
JOIN sys_fields sf
    ON val.field_code = sf.field_code
-- 仅保留必要排序（实际查询时建议在业务层排序）
ORDER BY sf.field_name, dv.msg_calc_dvc_time;