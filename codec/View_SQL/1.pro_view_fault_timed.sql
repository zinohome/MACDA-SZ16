CREATE VIEW pro_view_fault_timed AS
-- 合并预警（predict）和故障（error）数据
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    predict_start_time AS start_time,  -- 统一时间字段名为start_time
    predict_end_time AS end_time,      -- 统一时间字段名为end_time
    predict_status AS status,          -- 统一状态字段名为status
    '预警' AS fault_type               -- 标记来源为预警
FROM pro_view_predict_timed

UNION ALL  -- 合并数据（保留重复，比UNION更高效）

SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    error_start_time AS start_time,   -- 统一时间字段名为start_time
    error_end_time AS end_time,       -- 统一时间字段名为end_time
    error_status AS status,           -- 统一状态字段名为status
    '故障' AS fault_type               -- 标记来源为故障
FROM pro_view_error_timed;