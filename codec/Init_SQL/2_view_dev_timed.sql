-- 创建视图 dev_mview_error_changes
-- 创建预计算状态变化物化视图（每日刷新）
CREATE MATERIALIZED VIEW IF NOT EXISTS dev_mview_error_changes AS
WITH filtered_data AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        msg_calc_parse_time AT TIME ZONE 'Asia/Shanghai' AS event_time,
        param_value
    FROM
        dev_mview_error_transposed
    WHERE
        msg_calc_parse_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
)
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
    -- 标记状态变化点
    CASE
        WHEN LAG(param_value) OVER w != param_value THEN 1
        ELSE 0
    END AS is_change_point
FROM
    filtered_data
WINDOW w AS (PARTITION BY param_name ORDER BY event_time);

-- 添加必要的索引
CREATE INDEX idx_dev_error_changes ON dev_mview_error_changes (
    param_name, event_time, is_fault, is_change_point
);



-- 创建视图 dev_view_error_timed
-- 创建快速查询视图
CREATE OR REPLACE VIEW dev_view_error_timed AS
WITH
-- 1. 从预计算视图获取数据（仅最近7天）
recent_changes AS (
    SELECT *
    FROM dev_mview_error_changes
    WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
),

-- 2. 生成故障组ID（仅在状态变化时递增）
fault_groups AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time,
        is_fault,
        SUM(is_change_point) OVER (PARTITION BY param_name ORDER BY event_time) AS group_id
    FROM
        recent_changes
),

-- 3. 计算每组的起止时间
group_periods AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        group_id,
        MIN(event_time) AS fault_start_time,
        MAX(event_time) AS fault_end_candidate,
        BOOL_OR(is_fault = 1) AS has_fault
    FROM
        fault_groups
    GROUP BY
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        group_id
    HAVING
        BOOL_OR(is_fault = 1)
),

-- 4. 计算结束时间（使用窗口函数避免子查询）
final_periods AS (
    SELECT
        gp.*,
        -- 如果下一组的开始时间超过当前组结束时间+30分钟，则当前组结束
        CASE
            WHEN LEAD(gp.fault_start_time) OVER w
                 > gp.fault_end_candidate + INTERVAL '30 minutes'
            THEN gp.fault_end_candidate
            -- 否则，检查当前组结束时间是否超过当前时间-30分钟
            WHEN gp.fault_end_candidate < CURRENT_TIMESTAMP - INTERVAL '30 minutes'
            THEN gp.fault_end_candidate
            ELSE NULL
        END AS confirmed_end_time
    FROM
        group_periods gp
    WINDOW w AS (PARTITION BY gp.param_name ORDER BY gp.fault_start_time)
)

-- 5. 最终结果
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    fault_start_time,
    confirmed_end_time AS fault_end_time,
    CASE
        WHEN confirmed_end_time IS NULL THEN '持续中'
        ELSE '已结束'
    END AS fault_status,
    EXTRACT(EPOCH FROM (
        COALESCE(confirmed_end_time, CURRENT_TIMESTAMP) - fault_start_time
    )) / 60 AS total_minutes
FROM
    final_periods
WHERE
    has_fault
ORDER BY
    param_name,
    fault_start_time;