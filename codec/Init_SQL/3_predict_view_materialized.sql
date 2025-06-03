-- =============================================
-- 1. 创建基础物化视图（dev_mview_predict）
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS dev_mview_predict AS
SELECT
    msg_calc_dvc_time,
    msg_calc_parse_time,
    msg_calc_dvc_no,
    msg_calc_train_no,
    -- 计算字段（与原视图逻辑一致）
    msg_calc_train_no::INTEGER AS dvc_train_no,
    SUBSTRING(msg_calc_dvc_no FROM LENGTH(msg_calc_train_no) + 1)::INTEGER AS dvc_carriage_no,
    ref_leak_u11, ref_leak_u12, ref_leak_u21, ref_leak_u22,
    f_cp_u1, f_cp_u2, f_fas, f_ras, cabin_overtemp,
    f_presdiff_u1, f_presdiff_u2, f_ef_u11, f_ef_u12, f_ef_u21, f_ef_u22,
    f_cf_u11, f_cf_u12, f_cf_u21, f_cf_u22, f_fas_u11, f_fas_u12, f_fas_u21, f_fas_u22,
    f_aq_u1, f_aq_u2
FROM dev_predict;

-- 为高频查询字段添加索引
CREATE INDEX IF NOT EXISTS idx_dev_mview_predict_parse_time
    ON dev_mview_predict (msg_calc_parse_time);

CREATE INDEX IF NOT EXISTS idx_dev_mview_predict_train_no
    ON dev_mview_predict (dvc_train_no);


-- =============================================
-- 2. 创建转置物化视图（dev_mview_predict_transposed）
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS dev_mview_predict_transposed AS
SELECT
    dv.msg_calc_dvc_time,
    dv.msg_calc_parse_time,
    dv.msg_calc_dvc_no,
    dv.msg_calc_train_no,
    dv.dvc_train_no,
    dv.dvc_carriage_no,
    sf.field_name,
    val.field_value
FROM dev_mview_predict dv
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
JOIN sys_fields sf
    ON val.field_code = sf.field_code
ORDER BY sf.field_name, dv.msg_calc_parse_time;

-- 为转置物化视图添加索引
CREATE INDEX IF NOT EXISTS idx_dev_mview_predict_transposed_parse_time
    ON dev_mview_predict_transposed (msg_calc_parse_time);

CREATE INDEX IF NOT EXISTS idx_dev_mview_predict_transposed_field_name
    ON dev_mview_predict_transposed (field_name);


-- =============================================
-- 3. 物化视图刷新策略
-- =============================================
-- 手动刷新（视图名同步调整）
REFRESH MATERIALIZED VIEW dev_mview_predict;
REFRESH MATERIALIZED VIEW dev_mview_predict_transposed;

-- 定时自动刷新（视图名和任务名同步调整）
-- 1. 安装pg_cron（仅需执行一次）
CREATE EXTENSION IF NOT EXISTS pg_cron;
-- 2. 基础物化视图定时刷新（任务名调整为下划线）
SELECT cron.schedule(
    'refresh_dev_mview_predict',
    '0 3 * * *',
    'REFRESH MATERIALIZED VIEW dev_mview_predict;'
);
-- 3. 转置物化视图定时刷新（任务名保持下划线）
SELECT cron.schedule(
    'refresh_dev_mview_predict_transposed',
    '10 3 * * *',  -- 基础视图刷新后10分钟
    'REFRESH MATERIALIZED VIEW dev_mview_predict_transposed;'
);


-- =============================================
-- 1. 创建基础物化视图（pro_mview_predict）
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS pro_mview_predict AS
SELECT
    msg_calc_dvc_time,
    msg_calc_parse_time,
    msg_calc_dvc_no,
    msg_calc_train_no,
    -- 计算字段（与原视图逻辑一致）
    msg_calc_train_no::INTEGER AS dvc_train_no,
    SUBSTRING(msg_calc_dvc_no FROM LENGTH(msg_calc_train_no) + 1)::INTEGER AS dvc_carriage_no,
    ref_leak_u11, ref_leak_u12, ref_leak_u21, ref_leak_u22,
    f_cp_u1, f_cp_u2, f_fas, f_ras, cabin_overtemp,
    f_presdiff_u1, f_presdiff_u2, f_ef_u11, f_ef_u12, f_ef_u21, f_ef_u22,
    f_cf_u11, f_cf_u12, f_cf_u21, f_cf_u22, f_fas_u11, f_fas_u12, f_fas_u21, f_fas_u22,
    f_aq_u1, f_aq_u2
FROM pro_predict;

-- 为高频查询字段添加索引
CREATE INDEX IF NOT EXISTS idx_pro_mview_predict_dvc_time
    ON pro_mview_predict (msg_calc_dvc_time);

CREATE INDEX IF NOT EXISTS idx_pro_mview_predict_train_no
    ON pro_mview_predict (dvc_train_no);


-- =============================================
-- 2. 创建转置物化视图（pro_mview_predict_transposed）
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS pro_mview_predict_transposed AS
SELECT
    dv.msg_calc_dvc_time,
    dv.msg_calc_parse_time,
    dv.msg_calc_dvc_no,
    dv.msg_calc_train_no,
    dv.dvc_train_no,
    dv.dvc_carriage_no,
    sf.field_name,
    val.field_value
FROM pro_mview_predict dv  -- 引用调整后的基础物化视图名
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
JOIN sys_fields sf
    ON val.field_code = sf.field_code
ORDER BY sf.field_name, dv.msg_calc_dvc_time;

-- 为转置物化视图添加索引
CREATE INDEX IF NOT EXISTS idx_pro_mview_predict_transposed_dvc_time
    ON pro_mview_predict_transposed (msg_calc_dvc_time);

CREATE INDEX IF NOT EXISTS idx_pro_mview_predict_transposed_field_name
    ON pro_mview_predict_transposed (field_name);


-- =============================================
-- 3. 物化视图刷新策略
-- =============================================
-- 手动刷新（视图名同步调整）
REFRESH MATERIALIZED VIEW pro_mview_predict;
REFRESH MATERIALIZED VIEW pro_mview_predict_transposed;

-- 定时自动刷新（视图名和任务名同步调整）
-- 1. 安装pg_cron（仅需执行一次）
CREATE EXTENSION IF NOT EXISTS pg_cron;
-- 2. 基础物化视图定时刷新（任务名调整为下划线）
SELECT cron.schedule(
    'refresh_pro_mview_predict',
    '0 3 * * *',
    'REFRESH MATERIALIZED VIEW pro_mview_predict;'
);
-- 3. 转置物化视图定时刷新（任务名保持下划线）
SELECT cron.schedule(
    'refresh_pro_mview_predict_transposed',
    '10 3 * * *',  -- 基础视图刷新后10分钟
    'REFRESH MATERIALIZED VIEW pro_mview_predict_transposed;'
);


-- 创建视图 dev_mview_predict_changes
-- 创建预计算预测状态变化物化视图（每日刷新）
CREATE MATERIALIZED VIEW IF NOT EXISTS dev_mview_predict_changes AS
WITH filtered_data AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        field_name AS param_name,
        msg_calc_parse_time AT TIME ZONE 'Asia/Shanghai' AS event_time,
        field_value AS is_predicted
    FROM
        dev_mview_predict_transposed
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
    is_predicted,
    LAG(is_predicted) OVER w AS prev_status,
    LAG(event_time) OVER w AS prev_time,
    -- 标记状态变化点
    CASE
        WHEN LAG(is_predicted) OVER w != is_predicted THEN 1
        ELSE 0
    END AS is_change_point
FROM
    filtered_data
WINDOW w AS (PARTITION BY param_name ORDER BY event_time);

-- 添加必要的索引
CREATE INDEX idx_dev_predict_changes ON dev_mview_predict_changes (
    param_name, event_time, is_predicted, is_change_point
);


-- 创建视图 dev_view_predict_timed
-- 创建快速查询视图（毫秒级响应）
CREATE OR REPLACE VIEW dev_view_predict_timed AS
WITH
-- 1. 从预计算视图获取数据（仅最近7天）
recent_changes AS (
    SELECT *
    FROM dev_mview_predict_changes
    WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
),

-- 2. 生成预测组ID（仅在状态变化时递增）
prediction_groups AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time,
        is_predicted,
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
        MIN(event_time) AS prediction_start_time,
        MAX(event_time) AS prediction_end_candidate,
        BOOL_OR(is_predicted = 1) AS has_prediction
    FROM
        prediction_groups
    GROUP BY
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        group_id
    HAVING
        BOOL_OR(is_predicted = 1)
),

-- 4. 计算结束时间（使用窗口函数避免子查询）
final_periods AS (
    SELECT
        gp.*,
        -- 如果下一组的开始时间超过当前组结束时间+30分钟，则当前组结束
        CASE
            WHEN LEAD(gp.prediction_start_time) OVER w
                 > gp.prediction_end_candidate + INTERVAL '30 minutes'
            THEN gp.prediction_end_candidate
            -- 否则，检查当前组结束时间是否超过当前时间-30分钟
            WHEN gp.prediction_end_candidate < CURRENT_TIMESTAMP - INTERVAL '30 minutes'
            THEN gp.prediction_end_candidate
            ELSE NULL
        END AS confirmed_end_time
    FROM
        group_periods gp
    WINDOW w AS (PARTITION BY gp.param_name ORDER BY gp.prediction_start_time)
)

-- 5. 最终结果
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time,
    confirmed_end_time AS prediction_end_time,
    CASE
        WHEN confirmed_end_time IS NULL THEN '持续中'
        ELSE '已结束'
    END AS prediction_status,
    EXTRACT(EPOCH FROM (
        COALESCE(confirmed_end_time, CURRENT_TIMESTAMP) - prediction_start_time
    )) / 60 AS total_minutes
FROM
    final_periods
WHERE
    has_prediction
ORDER BY
    param_name,
    prediction_start_time;




-- 创建视图 pro_mview_predict_changes
-- 创建预计算预测状态变化物化视图（每日刷新）
CREATE MATERIALIZED VIEW IF NOT EXISTS pro_mview_predict_changes AS
WITH filtered_data AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        field_name AS param_name,
        msg_calc_dvc_time AT TIME ZONE 'Asia/Shanghai' AS event_time,
        field_value AS is_predicted
    FROM
        pro_mview_predict_transposed
    WHERE
        msg_calc_dvc_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
)
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    event_time,
    is_predicted,
    LAG(is_predicted) OVER w AS prev_status,
    LAG(event_time) OVER w AS prev_time,
    -- 标记状态变化点
    CASE
        WHEN LAG(is_predicted) OVER w != is_predicted THEN 1
        ELSE 0
    END AS is_change_point
FROM
    filtered_data
WINDOW w AS (PARTITION BY param_name ORDER BY event_time);

-- 添加必要的索引
CREATE INDEX idx_pro_predict_changes ON pro_mview_predict_changes (
    param_name, event_time, is_predicted, is_change_point
);


-- 创建视图 pro_view_predict_timed
-- 创建快速查询视图（毫秒级响应）
CREATE OR REPLACE VIEW pro_view_predict_timed AS
WITH
-- 1. 从预计算视图获取数据（仅最近7天）
recent_changes AS (
    SELECT *
    FROM pro_mview_predict_changes
    WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
),

-- 2. 生成预测组ID（仅在状态变化时递增）
prediction_groups AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time,
        is_predicted,
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
        MIN(event_time) AS prediction_start_time,
        MAX(event_time) AS prediction_end_candidate,
        BOOL_OR(is_predicted = 1) AS has_prediction
    FROM
        prediction_groups
    GROUP BY
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        group_id
    HAVING
        BOOL_OR(is_predicted = 1)
),

-- 4. 计算结束时间（使用窗口函数避免子查询）
final_periods AS (
    SELECT
        gp.*,
        -- 如果下一组的开始时间超过当前组结束时间+30分钟，则当前组结束
        CASE
            WHEN LEAD(gp.prediction_start_time) OVER w
                 > gp.prediction_end_candidate + INTERVAL '30 minutes'
            THEN gp.prediction_end_candidate
            -- 否则，检查当前组结束时间是否超过当前时间-30分钟
            WHEN gp.prediction_end_candidate < CURRENT_TIMESTAMP - INTERVAL '30 minutes'
            THEN gp.prediction_end_candidate
            ELSE NULL
        END AS confirmed_end_time
    FROM
        group_periods gp
    WINDOW w AS (PARTITION BY gp.param_name ORDER BY gp.prediction_start_time)
)

-- 5. 最终结果
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time,
    confirmed_end_time AS prediction_end_time,
    CASE
        WHEN confirmed_end_time IS NULL THEN '持续中'
        ELSE '已结束'
    END AS prediction_status,
    EXTRACT(EPOCH FROM (
        COALESCE(confirmed_end_time, CURRENT_TIMESTAMP) - prediction_start_time
    )) / 60 AS total_minutes
FROM
    final_periods
WHERE
    has_prediction
ORDER BY
    param_name,
    prediction_start_time;

