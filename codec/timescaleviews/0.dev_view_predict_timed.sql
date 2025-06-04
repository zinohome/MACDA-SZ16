CREATE VIEW dev_view_predict_timed AS  -- 视图名修改为dev_view_predict_timed
WITH
-- 步骤1：仅扫描最近30天的数据，并标记前后状态
event_boundaries AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        msg_calc_parse_time AS event_time,  -- 时序字段（与原表一致）
        param_value,  -- 预测状态值（1=预测发生，0=未发生）
        LAG(param_value) OVER (
            PARTITION BY msg_calc_dvc_no, msg_calc_train_no, dvc_train_no, dvc_carriage_no, param_name
            ORDER BY msg_calc_parse_time
        ) AS prev_value,
        LEAD(param_value) OVER (
            PARTITION BY msg_calc_dvc_no, msg_calc_train_no, dvc_train_no, dvc_carriage_no, param_name
            ORDER BY msg_calc_parse_time
        ) AS next_value,
        LEAD(msg_calc_parse_time) OVER (
            PARTITION BY msg_calc_dvc_no, msg_calc_train_no, dvc_train_no, dvc_carriage_no, param_name
            ORDER BY msg_calc_parse_time
        ) AS next_time
    FROM dev_predict_transposed  -- 原表名修改为dev_predict_transposed
    WHERE
        msg_calc_parse_time >= NOW() - INTERVAL '30 days'  -- 限制最近30天数据
),
-- 步骤2：筛选预测事件起始点（param_value=1且前一条非1）
predict_starts AS (  -- 逻辑与error_starts一致，重命名为predict_starts
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time AS predict_start_time  -- 起始时间字段名调整为predict_start_time
    FROM event_boundaries
    WHERE
        param_value = 1
        AND (prev_value != 1 OR prev_value IS NULL)  -- 新预测事件起点
),
-- 步骤3：为每个预测事件查找30分钟内的结束点（param_value=0）
predict_ends AS (  -- 逻辑与error_ends一致，重命名为predict_ends
    SELECT
        s.msg_calc_dvc_no,
        s.msg_calc_train_no,
        s.dvc_train_no,
        s.dvc_carriage_no,
        s.param_name,
        s.predict_start_time,
        MIN(e.msg_calc_parse_time) AS predict_end_time  -- 结束时间字段名调整为predict_end_time
    FROM predict_starts s
    LEFT JOIN dev_predict_transposed e  -- 关联原表dev_predict_transposed
        ON s.msg_calc_dvc_no = e.msg_calc_dvc_no
        AND s.msg_calc_train_no = e.msg_calc_train_no
        AND s.dvc_train_no = e.dvc_train_no
        AND s.dvc_carriage_no = e.dvc_carriage_no
        AND s.param_name = e.param_name
        AND e.param_value = 0  -- 预测结束需param_value=0
        AND e.msg_calc_parse_time > s.predict_start_time  -- 结束时间在预测开始后
        AND e.msg_calc_parse_time <= s.predict_start_time + INTERVAL '30 minutes'  -- 30分钟窗口内
    GROUP BY
        s.msg_calc_dvc_no, s.msg_calc_train_no, s.dvc_train_no,
        s.dvc_carriage_no, s.param_name, s.predict_start_time
)
-- 步骤4：生成最终视图（包含状态判断）
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    predict_start_time,  -- 起始时间字段
    predict_end_time,    -- 结束时间字段
    CASE
        WHEN predict_end_time IS NOT NULL THEN '结束'
        ELSE '持续'
    END AS predict_status  -- 状态字段名调整为predict_status
FROM predict_ends;