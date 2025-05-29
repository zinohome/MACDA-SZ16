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


-- 创建视图 dev_view_predict_timed
-- 创建视图 dev_view_predict_timed（修正持续中状态的结束时间）
CREATE OR REPLACE VIEW dev_view_predict_timed AS
WITH filtered_data AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        field_name AS param_name,
        msg_calc_parse_time AT TIME ZONE 'Asia/Shanghai' AS event_time,
        field_value AS param_value
    FROM
        dev_mview_predict_transposed
    WHERE
        msg_calc_parse_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai') - INTERVAL '7 days'
),
lagged_data AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time,
        param_value AS is_predicted,
        LAG(param_value) OVER w AS prev_status,
        LEAD(param_value) OVER w AS next_status,
        LEAD(event_time) OVER w AS next_time
    FROM
        filtered_data
    WINDOW w AS (PARTITION BY param_name ORDER BY event_time)
),
potential_ends AS (
    SELECT
        *,
        EXISTS (
            SELECT 1
            FROM filtered_data f2
            WHERE
                f2.param_name = lagged_data.param_name
                AND f2.event_time > lagged_data.event_time
                AND f2.event_time <= lagged_data.event_time + INTERVAL '30 minutes'
                AND f2.param_value = 1
        ) AS has_retrigger_within_30min
    FROM
        lagged_data
    WHERE
        is_predicted = 0 AND prev_status = 1
),
confirmed_ends AS (
    SELECT
        param_name,
        event_time AS confirmed_end_time
    FROM
        potential_ends
    WHERE
        has_retrigger_within_30min = FALSE
),
status_groups AS (
    SELECT
        l.msg_calc_dvc_no,
        l.msg_calc_train_no,
        l.dvc_train_no,
        l.dvc_carriage_no,
        l.param_name,
        l.event_time,
        l.is_predicted,
        e.confirmed_end_time,
        SUM(
            CASE
                WHEN l.prev_status = 0 AND l.is_predicted = 1 THEN 1
                WHEN e.confirmed_end_time IS NOT NULL THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY l.param_name ORDER BY l.event_time) AS status_group
    FROM
        lagged_data l
    LEFT JOIN
        confirmed_ends e
        ON l.param_name = e.param_name AND l.event_time = e.confirmed_end_time
),
prediction_periods AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        MIN(CASE WHEN is_predicted = 1 THEN event_time END) AS prediction_start_time,
        MAX(CASE WHEN is_predicted = 1 THEN event_time END) AS prediction_end_time_candidate,
        MAX(confirmed_end_time) AS confirmed_end_time,
        COUNT(*) FILTER (WHERE is_predicted = 1) > 0 AS has_active_prediction
    FROM
        status_groups
    GROUP BY
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        status_group
),
final_periods AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        prediction_start_time,
        -- 关键修正：当状态为"持续中"时，强制将end_time设为NULL
        CASE
            WHEN confirmed_end_time IS NULL AND prediction_end_time_candidate IS NOT NULL THEN NULL
            ELSE COALESCE(confirmed_end_time, prediction_end_time_candidate)
        END AS prediction_end_time,
        CASE
            WHEN confirmed_end_time IS NULL AND prediction_end_time_candidate IS NOT NULL THEN '持续中'
            ELSE '已结束'
        END AS prediction_status,
        EXTRACT(EPOCH FROM (
            COALESCE(confirmed_end_time, prediction_end_time_candidate) - prediction_start_time
        )) / 60 AS total_minutes
    FROM
        prediction_periods
    WHERE
        has_active_prediction
)
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time,
    prediction_end_time,
    prediction_status,
    total_minutes
FROM
    final_periods
ORDER BY
    param_name,
    prediction_start_time;



-- 创建视图 pro_view_predict_timed
CREATE OR REPLACE VIEW pro_view_predict_timed AS
WITH filtered_data AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        field_name AS param_name,
        msg_calc_dvc_time AT TIME ZONE 'Asia/Shanghai' AS event_time,
        field_value AS param_value
    FROM
        pro_mview_predict_transposed
    WHERE
        msg_calc_dvc_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai') - INTERVAL '7 days'
),
lagged_data AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        event_time,
        param_value AS is_predicted,
        LAG(param_value) OVER w AS prev_status,
        LEAD(param_value) OVER w AS next_status,
        LEAD(event_time) OVER w AS next_time
    FROM
        filtered_data
    WINDOW w AS (PARTITION BY param_name ORDER BY event_time)
),
potential_ends AS (
    SELECT
        *,
        EXISTS (
            SELECT 1
            FROM filtered_data f2
            WHERE
                f2.param_name = lagged_data.param_name
                AND f2.event_time > lagged_data.event_time
                AND f2.event_time <= lagged_data.event_time + INTERVAL '30 minutes'
                AND f2.param_value = 1
        ) AS has_retrigger_within_30min
    FROM
        lagged_data
    WHERE
        is_predicted = 0 AND prev_status = 1
),
confirmed_ends AS (
    SELECT
        param_name,
        event_time AS confirmed_end_time
    FROM
        potential_ends
    WHERE
        has_retrigger_within_30min = FALSE
),
status_groups AS (
    SELECT
        l.msg_calc_dvc_no,
        l.msg_calc_train_no,
        l.dvc_train_no,
        l.dvc_carriage_no,
        l.param_name,
        l.event_time,
        l.is_predicted,
        e.confirmed_end_time,
        SUM(
            CASE
                WHEN l.prev_status = 0 AND l.is_predicted = 1 THEN 1
                WHEN e.confirmed_end_time IS NOT NULL THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY l.param_name ORDER BY l.event_time) AS status_group
    FROM
        lagged_data l
    LEFT JOIN
        confirmed_ends e
        ON l.param_name = e.param_name AND l.event_time = e.confirmed_end_time
),
prediction_periods AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        MIN(CASE WHEN is_predicted = 1 THEN event_time END) AS prediction_start_time,
        MAX(CASE WHEN is_predicted = 1 THEN event_time END) AS prediction_end_time_candidate,
        MAX(confirmed_end_time) AS confirmed_end_time,
        COUNT(*) FILTER (WHERE is_predicted = 1) > 0 AS has_active_prediction
    FROM
        status_groups
    GROUP BY
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        status_group
),
final_periods AS (
    SELECT
        msg_calc_dvc_no,
        msg_calc_train_no,
        dvc_train_no,
        dvc_carriage_no,
        param_name,
        prediction_start_time,
        -- 关键修正：当状态为"持续中"时，强制将end_time设为NULL
        CASE
            WHEN confirmed_end_time IS NULL AND prediction_end_time_candidate IS NOT NULL THEN NULL
            ELSE COALESCE(confirmed_end_time, prediction_end_time_candidate)
        END AS prediction_end_time,
        CASE
            WHEN confirmed_end_time IS NULL AND prediction_end_time_candidate IS NOT NULL THEN '持续中'
            ELSE '已结束'
        END AS prediction_status,
        EXTRACT(EPOCH FROM (
            COALESCE(confirmed_end_time, prediction_end_time_candidate) - prediction_start_time
        )) / 60 AS total_minutes
    FROM
        prediction_periods
    WHERE
        has_active_prediction
)
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    prediction_start_time,
    prediction_end_time,
    prediction_status,
    total_minutes
FROM
    final_periods
ORDER BY
    param_name,
    prediction_start_time;