-- 创建合并视图 dev_view_fault_timed
CREATE OR REPLACE VIEW dev_view_fault_timed AS
SELECT
    '预测' AS fault_type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time AS start_time,
    prediction_end_time AS end_time,
    prediction_status AS status,
    total_minutes
FROM
    dev_view_predict_timed
UNION ALL
SELECT
    '故障' AS fault_type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    fault_start_time AS start_time,
    fault_end_time AS end_time,
    fault_status AS status,
    total_minutes
FROM
    dev_view_error_timed
ORDER BY
    fault_type,
    start_time;


-- 创建合并视图 pro_view_fault_timed
CREATE OR REPLACE VIEW pro_view_fault_timed AS
SELECT
    '预测' AS fault_type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time AS start_time,
    prediction_end_time AS end_time,
    prediction_status AS status,
    total_minutes
FROM
    pro_view_predict_timed
UNION ALL
SELECT
    '故障' AS fault_type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    fault_start_time AS start_time,
    fault_end_time AS end_time,
    fault_status AS status,
    total_minutes
FROM
    pro_view_error_timed
ORDER BY
    fault_type,
    start_time;