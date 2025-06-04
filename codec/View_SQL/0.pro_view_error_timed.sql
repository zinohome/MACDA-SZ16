CREATE VIEW pro_view_error_timed AS
WITH
-- 步骤1：仅扫描最近30天的数据，并标记前后状态
event_boundaries AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        msg_calc_dvc_time AS event_time,  -- 原始表时间字段的别名
        param_value,
        LAG(param_value) OVER (
            PARTITION BY msg_calc_dvc_no, msg_calc_train_no, dvc_train_no, dvc_carriage_no, param_name
            ORDER BY msg_calc_dvc_time
        ) AS prev_value,
        LEAD(param_value) OVER (
            PARTITION BY msg_calc_dvc_no, msg_calc_train_no, dvc_train_no, dvc_carriage_no, param_name
            ORDER BY msg_calc_dvc_time
        ) AS next_value,
        LEAD(msg_calc_dvc_time) OVER (
            PARTITION BY msg_calc_dvc_no, msg_calc_train_no, dvc_train_no, dvc_carriage_no, param_name
            ORDER BY msg_calc_dvc_time
        ) AS next_time
    FROM pro_error_transposed
    WHERE
        msg_calc_dvc_time >= NOW() - INTERVAL '30 days'  -- 限制最近30天数据
),
-- 步骤2：筛选故障事件起始点（param_value=1且前一条非1）
error_starts AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time AS error_start_time  -- 来自event_boundaries的event_time（即msg_calc_dvc_time）
    FROM event_boundaries
    WHERE
        param_value = 1
        AND (prev_value != 1 OR prev_value IS NULL)  -- 新故障事件起点
),
-- 步骤3：为每个故障事件查找30分钟内的结束点（修正列名错误）
error_ends AS (
    SELECT
        s.msg_calc_dvc_no,
        s.msg_calc_train_no,
        s.dvc_train_no,
        s.dvc_carriage_no,
        s.param_name,
        s.error_start_time,
        -- 改为使用原始表的msg_calc_dvc_time字段（原event_time是别名）
        MIN(e.msg_calc_dvc_time) AS error_end_time
    FROM error_starts s
    LEFT JOIN pro_error_transposed e  -- 关联原始表（非event_boundaries）
        ON s.msg_calc_dvc_no = e.msg_calc_dvc_no
        AND s.msg_calc_train_no = e.msg_calc_train_no
        AND s.dvc_train_no = e.dvc_train_no
        AND s.dvc_carriage_no = e.dvc_carriage_no
        AND s.param_name = e.param_name
        AND e.param_value = 0  -- 结束事件需param_value=0
        AND e.msg_calc_dvc_time > s.error_start_time  -- 结束时间在故障开始后
        AND e.msg_calc_dvc_time <= s.error_start_time + INTERVAL '30 minutes'  -- 30分钟窗口内
    GROUP BY
        s.msg_calc_dvc_no, s.msg_calc_train_no, s.dvc_train_no,
        s.dvc_carriage_no, s.param_name, s.error_start_time
)
-- 步骤4：生成最终视图（包含状态判断）
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    error_start_time,
    error_end_time,
    CASE
        WHEN error_end_time IS NOT NULL THEN '结束'
        ELSE '持续'
    END AS error_status
FROM error_ends;