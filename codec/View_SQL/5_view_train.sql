-- 创建统计列车状态的视图 dev_view_train_num
CREATE OR REPLACE VIEW dev_view_train_num AS
SELECT
    SUM(CASE WHEN status = 'normal' THEN 1 ELSE 0 END) AS normal_operation,
    SUM(CASE WHEN status = 'planned' THEN 1 ELSE 0 END) AS planned_maintenance,
    SUM(CASE WHEN status = 'fault' THEN 1 ELSE 0 END) AS immediate_repair
FROM (
    SELECT
        train.dvc_train_no,
        CASE
            WHEN BOOL_OR(fault.fault_type = '故障') THEN 'fault'
            WHEN BOOL_OR(fault.fault_type = '预警') THEN 'planned'
            ELSE 'normal'
        END AS status
    FROM dev_macda_ac train
    LEFT JOIN dev_view_fault_timed_mat fault
        ON train.dvc_train_no = fault.dvc_train_no
        AND train.dvc_carriage_no = fault.dvc_carriage_no
        AND (fault.status = '持续')
    GROUP BY train.dvc_train_no
) train_status;

-- 创建统计列车状态的视图 pro_view_train_num
CREATE OR REPLACE VIEW pro_view_train_num AS
SELECT
    SUM(CASE WHEN status = 'normal' THEN 1 ELSE 0 END) AS normal_operation,
    SUM(CASE WHEN status = 'planned' THEN 1 ELSE 0 END) AS planned_maintenance,
    SUM(CASE WHEN status = 'fault' THEN 1 ELSE 0 END) AS immediate_repair
FROM (
    SELECT
        train.dvc_train_no,
        CASE
            WHEN BOOL_OR(fault.fault_type = '故障') THEN 'fault'
            WHEN BOOL_OR(fault.fault_type = '预警') THEN 'planned'
            ELSE 'normal'
        END AS status
    FROM pro_macda_ac train
    LEFT JOIN pro_view_fault_timed_mat fault
        ON train.dvc_train_no = fault.dvc_train_no
        AND train.dvc_carriage_no = fault.dvc_carriage_no
        AND (fault.status = '持续')
    GROUP BY train.dvc_train_no
) train_status;


-- 创建统计列车状态的视图 dev_view_carriage_num
CREATE OR REPLACE VIEW dev_view_carriage_num AS
WITH carriage_status AS (
    SELECT
        dt.dvc_train_no,
        dt.dvc_carriage_no,
        CASE
            -- 优先级: 故障 > 预警 > 正常
            WHEN BOOL_OR(dvft.fault_type = '故障') THEN 'immediate_repair'
            WHEN BOOL_OR(dvft.fault_type = '预警') THEN 'planned_maintenance'
            ELSE 'normal'
        END AS status
    FROM
        dev_macda_ac dt
    LEFT JOIN
        dev_view_fault_timed_mat dvft
        ON dt.dvc_train_no = dvft.dvc_train_no
        AND dt.dvc_carriage_no = dvft.dvc_carriage_no
        AND (dvft.status = '持续')
    GROUP BY
        dt.dvc_train_no, dt.dvc_carriage_no
)
SELECT
    dvc_train_no,
    COUNT(CASE WHEN status = 'normal' THEN 1 END) AS normal_count,
    COUNT(CASE WHEN status = 'planned_maintenance' THEN 1 END) AS planned_maintenance_count,
    COUNT(CASE WHEN status = 'immediate_repair' THEN 1 END) AS immediate_repair_count
FROM
    carriage_status
GROUP BY
    dvc_train_no;


-- 创建统计列车状态的视图 pro_view_carriage_num
CREATE OR REPLACE VIEW pro_view_carriage_num AS
WITH carriage_status AS (
    SELECT
        dt.dvc_train_no,
        dt.dvc_carriage_no,
        CASE
            -- 优先级: 故障 > 预警 > 正常
            WHEN BOOL_OR(dvft.fault_type = '故障') THEN 'immediate_repair'
            WHEN BOOL_OR(dvft.fault_type = '预警') THEN 'planned_maintenance'
            ELSE 'normal'
        END AS status
    FROM
        pro_macda_ac dt
    LEFT JOIN
        pro_view_fault_timed_mat dvft
        ON dt.dvc_train_no = dvft.dvc_train_no
        AND dt.dvc_carriage_no = dvft.dvc_carriage_no
        AND (dvft.status = '持续')
    GROUP BY
        dt.dvc_train_no, dt.dvc_carriage_no
)
SELECT
    dvc_train_no,
    COUNT(CASE WHEN status = 'normal' THEN 1 END) AS normal_count,
    COUNT(CASE WHEN status = 'planned_maintenance' THEN 1 END) AS planned_maintenance_count,
    COUNT(CASE WHEN status = 'immediate_repair' THEN 1 END) AS immediate_repair_count
FROM
    carriage_status
GROUP BY
    dvc_train_no;