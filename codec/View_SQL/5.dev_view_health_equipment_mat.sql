CREATE MATERIALIZED VIEW dev_view_health_equipment_mat AS
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
    FROM dev_statistics_transposed dst
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
    msg_calc_parse_time, -- 新增到最终结果
    health_status
FROM equipment_health
WHERE failure_reason IS NULL;

-- 创建必要的索引来提高性能
CREATE INDEX idx_dev_stats_identification ON dev_statistics_transposed (
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no
);

CREATE INDEX idx_dev_stats_param_time ON dev_statistics_transposed (
    param_name,
    msg_calc_parse_time DESC
);

CREATE INDEX idx_equipment_associated ON equipment_management (associated_data);

CREATE UNIQUE INDEX idx_dev_unique_health_equipment ON dev_view_health_equipment_mat (
    msg_calc_dvc_no,
    msg_calc_train_no,
    dvc_train_no,
    dvc_carriage_no,
    param_name,
    msg_calc_parse_time,
    health_status
);