-- 创建合并视图 dev_view_fault_timed
CREATE OR REPLACE VIEW dev_view_fault_timed AS
SELECT
    '故障' AS type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    fault_start_time AS event_start_time,
    fault_end_time AS event_end_time,
    fault_status AS event_status,
    sub_events,
    total_minutes
FROM
    dev_view_error_timed

UNION ALL

SELECT
    '预警' AS type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time AS event_start_time,
    prediction_end_time AS event_end_time,
    prediction_status AS event_status,
    sub_events,
    total_minutes
FROM
    dev_view_predict_timed

ORDER BY
    param_name,
    event_start_time;


-- 创建合并视图 pro_view_fault_timed
CREATE OR REPLACE VIEW pro_view_fault_timed AS
SELECT
    '故障' AS type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    fault_start_time AS event_start_time,
    fault_end_time AS event_end_time,
    fault_status AS event_status,
    sub_events,
    total_minutes
FROM
    pro_view_error_timed

UNION ALL

SELECT
    '预警' AS type,
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time AS event_start_time,
    prediction_end_time AS event_end_time,
    prediction_status AS event_status,
    sub_events,
    total_minutes
FROM
    pro_view_predict_timed

ORDER BY
    param_name,
    event_start_time;