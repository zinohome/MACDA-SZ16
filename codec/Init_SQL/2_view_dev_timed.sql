-- 创建视图 dev_view_error_timed
CREATE OR REPLACE VIEW dev_view_error_timed AS
WITH filtered_data AS (
    -- 1. 过滤近7天数据 + 故障状态
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        msg_calc_parse_time AT TIME ZONE 'Asia/Shanghai' AS event_time,  -- 转换为上海时间
        param_value
    FROM
        dev_mview_error_transposed
    WHERE
        msg_calc_parse_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai') - INTERVAL '7 days'  -- 近7天
    ORDER BY
        param_name, msg_calc_parse_time  -- 提前排序
),
status_changes AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time,
        param_value AS is_fault,
        LAG(param_value) OVER w AS prev_status,
        LAG(event_time) OVER w AS prev_time,
        LEAD(param_value) OVER w AS next_status,
        ROW_NUMBER() OVER w AS rn
    FROM
        filtered_data
    WINDOW w AS (PARTITION BY param_name ORDER BY event_time)
),
fault_periods AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time AS fault_start_time,
        LEAD(event_time) OVER (PARTITION BY param_name ORDER BY rn) AS fault_end_time,
        is_fault,
        -- 计算与上一次故障的时间间隔
        EXTRACT(EPOCH FROM (event_time - prev_time)) / 60 AS minutes_since_last
    FROM
        status_changes
    WHERE
        (prev_status = 0 AND is_fault = 1) -- 故障开始
        OR (is_fault = 1 AND next_status IS NULL) -- 最后一条记录为故障
),
time_filtered_faults AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        fault_start_time,
        fault_end_time,
        is_fault,
        minutes_since_last,
        -- 使用时间阈值判断是否为新故障
        CASE
            WHEN minutes_since_last > 30 OR minutes_since_last IS NULL THEN 1
            ELSE 0
        END AS is_new_fault,
        -- 为每个故障分配组ID
        SUM(
            CASE
                WHEN minutes_since_last > 30 OR minutes_since_last IS NULL THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY param_name ORDER BY fault_start_time) AS fault_group
    FROM
        fault_periods
    WHERE
        is_fault = 1
)
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    MIN(fault_start_time) AS fault_start_time,
    MAX(fault_end_time) AS fault_end_time,
    CASE
        WHEN MAX(fault_end_time) IS NULL THEN '持续中'
        ELSE '已结束'
    END AS fault_status,
    COUNT(*) AS sub_events,
    EXTRACT(EPOCH FROM (MAX(fault_end_time) - MIN(fault_start_time))) / 60 AS total_minutes
FROM
    time_filtered_faults
GROUP BY
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    fault_group
ORDER BY
    param_name,
    fault_start_time;