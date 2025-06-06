CREATE MATERIALIZED VIEW pro_view_health_equipment_mat AS
WITH latest_stats AS (
    SELECT
        dst.msg_calc_dvc_no,
        dst.msg_calc_train_no,
        dst.dvc_train_no,
        dst.dvc_carriage_no,
        dst.param_name,
        dst.param_value,
        dst.msg_calc_parse_time, -- 保留时间字段
        ROW_NUMBER() OVER (
            PARTITION BY
                dst.msg_calc_dvc_no,
                dst.msg_calc_train_no,
                dst.dvc_train_no,
                dst.dvc_carriage_no,
                dst.param_name
            ORDER BY dst.msg_calc_parse_time DESC
        ) AS rn
    FROM pro_statistics_transposed dst
),
equipment_health AS (
    SELECT
        ls.msg_calc_dvc_no,
        ls.msg_calc_train_no,
        ls.dvc_train_no,
        ls.dvc_carriage_no,
        COALESCE(ls.param_name, em.associated_data) AS param_name,
        em.baseline_data,
        em.health_threshold,
        em.sub_health_threshold,
        ls.param_value AS actual_value,
        ls.msg_calc_parse_time, -- 新增时间字段
        CASE
            WHEN ls.param_value::numeric < em.baseline_data::numeric * em.health_threshold::numeric THEN '健康'
            WHEN ls.param_value::numeric BETWEEN em.baseline_data::numeric * em.health_threshold::numeric
                                           AND em.baseline_data::numeric * em.sub_health_threshold::numeric THEN '亚健康'
            WHEN ls.param_value::numeric > em.baseline_data::numeric * em.sub_health_threshold::numeric THEN '非健康'
            ELSE '评估失败'
        END AS health_status,
        ROUND(ls.param_value::numeric / em.baseline_data::numeric, 2) AS life_ratio, -- 新增寿命比例字段(两位小数)
        CASE
            WHEN ls.param_name IS NULL THEN '无匹配数据'
            WHEN NOT (ls.param_value::text ~ '^[0-9]+(\.[0-9]+)?$') THEN '数据格式错误'
            ELSE NULL
        END AS failure_reason
    FROM equipment_management em
    LEFT JOIN latest_stats ls
        ON em.associated_data = ls.param_name
        AND ls.rn = 1
)
SELECT
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    msg_calc_parse_time,
    health_status,
    actual_value AS param_value, -- 修正：使用actual_value列并将其重命名为param_value
    CASE
        WHEN baseline_data::numeric IN (90000000, 180000000) THEN ROUND(baseline_data::numeric / 3600, 0)
        WHEN baseline_data::numeric = 10000000 THEN ROUND(baseline_data::numeric / 10000, 0)
        ELSE baseline_data::numeric
    END AS baseline_data,
    life_ratio
FROM equipment_health
WHERE failure_reason IS NULL;

CREATE UNIQUE INDEX idx_pro_unique_health_equipment ON pro_view_health_equipment_mat (
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    msg_calc_parse_time,
    health_status
);

-- 创建必要的索引来提高性能
CREATE INDEX idx_pro_stats_identification ON pro_statistics_transposed (
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no
);

CREATE INDEX idx_pro_stats_param_time ON pro_statistics_transposed (
    param_name,
    msg_calc_parse_time DESC
);

CREATE INDEX idx_equipment_associated ON equipment_management (associated_data);