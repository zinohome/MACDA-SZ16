-- 创建/替换视图：包含最新工况及扩展统计字段
CREATE OR REPLACE VIEW dev_view_train_opstatus AS
WITH
-- 子查询1：获取所有唯一列车号（来自主表）
unique_trains AS (
    SELECT DISTINCT dvc_train_no
    FROM dev_macda_ac
),
-- 子查询2：获取每个列车的最新工况记录（原逻辑）
latest_conditions AS (
    SELECT
        dvc_train_no,
        dvc_op_condition AS latest_op_condition,
        msg_calc_parse_time AS latest_time,
        ROW_NUMBER() OVER (
            PARTITION BY dvc_train_no
            ORDER BY msg_calc_parse_time DESC
        ) AS rn
    FROM dev_macda
),
-- 子查询3：统计故障/预警数量（来自dev_view_fault_timed_mat）
fault_stats AS (
    SELECT
        dvc_train_no,
        COUNT(CASE WHEN fault_type = '故障' THEN 1 END) AS immediate_repair_cnt,  -- 立即维修（故障数）
        COUNT(CASE WHEN fault_type = '预警' THEN 1 END) AS plan_tracking_cnt       -- 计划跟踪（预警数）
    FROM dev_view_fault_timed_mat
    GROUP BY dvc_train_no
),
-- 子查询4：统计非健康设备数量（来自dev_view_health_equipment_mat）
health_stats AS (
    SELECT
        dvc_train_no,
        COUNT(CASE WHEN health_status = '非健康' THEN 1 END) AS plan_repair_cnt  -- 计划维修（非健康数）
    FROM dev_view_health_equipment_mat
    GROUP BY dvc_train_no
)

-- 主查询：关联所有数据
SELECT
    ut.dvc_train_no,
    lc.latest_op_condition,
    lc.latest_time,
    COALESCE(fs.immediate_repair_cnt, 0) AS "立即维修",  -- 故障数（无记录时为0）
    COALESCE(fs.plan_tracking_cnt, 0) AS "加强跟踪",     -- 预警数（无记录时为0）
    COALESCE(hs.plan_repair_cnt, 0) AS "计划维修",        -- 非健康数（无记录时为0）
    CASE
        WHEN COALESCE(fs.immediate_repair_cnt, 0) = 0
         AND COALESCE(fs.plan_tracking_cnt, 0) = 0
         AND COALESCE(hs.plan_repair_cnt, 0) = 0
        THEN 1
        ELSE 0
    END AS "正常运营"
FROM unique_trains ut
LEFT JOIN latest_conditions lc
    ON ut.dvc_train_no = lc.dvc_train_no AND lc.rn = 1
LEFT JOIN fault_stats fs
    ON ut.dvc_train_no = fs.dvc_train_no
LEFT JOIN health_stats hs
    ON ut.dvc_train_no = hs.dvc_train_no;

-- （可选）保留原索引优化建议（提升基础表查询性能）
CREATE INDEX IF NOT EXISTS idx_dev_macda_train_time
ON dev_macda (dvc_train_no, msg_calc_parse_time DESC);


-- 创建/替换视图：包含最新工况及扩展统计字段
CREATE OR REPLACE VIEW pro_view_train_opstatus AS
WITH
-- 子查询1：获取所有唯一列车号（来自主表）
unique_trains AS (
    SELECT DISTINCT dvc_train_no
    FROM pro_macda_ac
),
-- 子查询2：获取每个列车的最新工况记录（原逻辑）
latest_conditions AS (
    SELECT
        dvc_train_no,
        dvc_op_condition AS latest_op_condition,
        msg_calc_dvc_time AS latest_time,
        ROW_NUMBER() OVER (
            PARTITION BY dvc_train_no
            ORDER BY msg_calc_dvc_time DESC
        ) AS rn
    FROM pro_macda
),
-- 子查询3：统计故障/预警数量（来自dev_view_fault_timed_mat）
fault_stats AS (
    SELECT
        dvc_train_no,
        COUNT(CASE WHEN fault_type = '故障' THEN 1 END) AS immediate_repair_cnt,  -- 立即维修（故障数）
        COUNT(CASE WHEN fault_type = '预警' THEN 1 END) AS plan_tracking_cnt       -- 计划跟踪（预警数）
    FROM pro_view_fault_timed_mat
    GROUP BY dvc_train_no
),
-- 子查询4：统计非健康设备数量（来自dev_view_health_equipment_mat）
health_stats AS (
    SELECT
        dvc_train_no,
        COUNT(CASE WHEN health_status = '非健康' THEN 1 END) AS plan_repair_cnt  -- 计划维修（非健康数）
    FROM pro_view_health_equipment_mat
    GROUP BY dvc_train_no
)

-- 主查询：关联所有数据
SELECT
    ut.dvc_train_no,
    lc.latest_op_condition,
    lc.latest_time,
    COALESCE(fs.immediate_repair_cnt, 0) AS "立即维修",  -- 故障数（无记录时为0）
    COALESCE(fs.plan_tracking_cnt, 0) AS "加强跟踪",     -- 预警数（无记录时为0）
    COALESCE(hs.plan_repair_cnt, 0) AS "计划维修",        -- 非健康数（无记录时为0）
    CASE
        WHEN COALESCE(fs.immediate_repair_cnt, 0) = 0
         AND COALESCE(fs.plan_tracking_cnt, 0) = 0
         AND COALESCE(hs.plan_repair_cnt, 0) = 0
        THEN 1
        ELSE 0
    END AS "正常运营"
FROM unique_trains ut
LEFT JOIN latest_conditions lc
    ON ut.dvc_train_no = lc.dvc_train_no AND lc.rn = 1
LEFT JOIN fault_stats fs
    ON ut.dvc_train_no = fs.dvc_train_no
LEFT JOIN health_stats hs
    ON ut.dvc_train_no = hs.dvc_train_no;

-- （可选）保留原索引优化建议（提升基础表查询性能）
CREATE INDEX IF NOT EXISTS idx_pro_macda_train_time
ON pro_macda (dvc_train_no, msg_calc_parse_time DESC);
