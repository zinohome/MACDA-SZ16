CREATE OR REPLACE VIEW dev_macda_ac AS
SELECT DISTINCT dvc_train_no, dvc_carriage_no
FROM dev_macda
WHERE msg_calc_parse_time >= NOW() - INTERVAL '30 minutes';


CREATE OR REPLACE VIEW pro_macda_ac AS
SELECT DISTINCT dvc_train_no, dvc_carriage_no
FROM pro_macda
WHERE msg_calc_dvc_time >= NOW() - INTERVAL '30 minutes';


CREATE OR REPLACE VIEW dev_view_train_opstatus as
WITH unique_trains AS (
         SELECT DISTINCT dev_macda_ac.dvc_train_no
           FROM dev_macda_ac
        ), latest_conditions AS (
         SELECT dev_macda.dvc_train_no,
            dev_macda.dvc_op_condition AS latest_op_condition,
            dev_macda.msg_calc_parse_time AS latest_time,
            row_number() OVER (PARTITION BY dev_macda.dvc_train_no ORDER BY dev_macda.msg_calc_parse_time DESC) AS rn
           FROM dev_macda
		   WHERE dev_macda.msg_calc_parse_time >= NOW() - INTERVAL '30 minutes'
        ), fault_stats AS (
         SELECT dev_view_fault_timed_mat.dvc_train_no,
            count(
                CASE
                    WHEN dev_view_fault_timed_mat.fault_type = '故障'::text THEN 1
                    ELSE NULL::integer
                END) AS immediate_repair_cnt,
            count(
                CASE
                    WHEN dev_view_fault_timed_mat.fault_type = '预警'::text THEN 1
                    ELSE NULL::integer
                END) AS plan_tracking_cnt
           FROM dev_view_fault_timed_mat
          GROUP BY dev_view_fault_timed_mat.dvc_train_no
        ), health_stats AS (
         SELECT dev_view_health_equipment_mat.dvc_train_no,
            count(
                CASE
                    WHEN dev_view_health_equipment_mat.health_status = '非健康'::text THEN 1
                    ELSE NULL::integer
                END) AS plan_repair_cnt
           FROM dev_view_health_equipment_mat
          GROUP BY dev_view_health_equipment_mat.dvc_train_no
        )
 SELECT ut.dvc_train_no,
    lc.latest_op_condition,
    lc.latest_time,
    COALESCE(fs.immediate_repair_cnt, 0::bigint) AS "立即维修",
    COALESCE(fs.plan_tracking_cnt, 0::bigint) AS "加强跟踪",
    COALESCE(hs.plan_repair_cnt, 0::bigint) AS "计划维修",
        CASE
            WHEN COALESCE(fs.immediate_repair_cnt, 0::bigint) = 0 AND COALESCE(fs.plan_tracking_cnt, 0::bigint) = 0 AND COALESCE(hs.plan_repair_cnt, 0::bigint) = 0 THEN 1
            ELSE 0
        END AS "正常运营"
   FROM unique_trains ut
     LEFT JOIN latest_conditions lc ON ut.dvc_train_no = lc.dvc_train_no AND lc.rn = 1
     LEFT JOIN fault_stats fs ON ut.dvc_train_no = fs.dvc_train_no
     LEFT JOIN health_stats hs ON ut.dvc_train_no = hs.dvc_train_no;



CREATE OR REPLACE VIEW pro_view_train_opstatus as
 WITH unique_trains AS (
         SELECT DISTINCT pro_macda_ac.dvc_train_no
           FROM pro_macda_ac
        ), latest_conditions AS (
         SELECT pro_macda.dvc_train_no,
            pro_macda.dvc_op_condition AS latest_op_condition,
            pro_macda.msg_calc_dvc_time AS latest_time,
            row_number() OVER (PARTITION BY pro_macda.dvc_train_no ORDER BY pro_macda.msg_calc_dvc_time DESC) AS rn
           FROM pro_macda
		   WHERE pro_macda.msg_calc_dvc_time >= NOW() - INTERVAL '30 minutes'
        ), fault_stats AS (
         SELECT pro_view_fault_timed_mat.dvc_train_no,
            count(
                CASE
                    WHEN pro_view_fault_timed_mat.fault_type = '故障'::text THEN 1
                    ELSE NULL::integer
                END) AS immediate_repair_cnt,
            count(
                CASE
                    WHEN pro_view_fault_timed_mat.fault_type = '预警'::text THEN 1
                    ELSE NULL::integer
                END) AS plan_tracking_cnt
           FROM pro_view_fault_timed_mat
          GROUP BY pro_view_fault_timed_mat.dvc_train_no
        ), health_stats AS (
         SELECT pro_view_health_equipment_mat.dvc_train_no,
            count(
                CASE
                    WHEN pro_view_health_equipment_mat.health_status = '非健康'::text THEN 1
                    ELSE NULL::integer
                END) AS plan_repair_cnt
           FROM pro_view_health_equipment_mat
          GROUP BY pro_view_health_equipment_mat.dvc_train_no
        )
 SELECT ut.dvc_train_no,
    lc.latest_op_condition,
    lc.latest_time,
    COALESCE(fs.immediate_repair_cnt, 0::bigint) AS "立即维修",
    COALESCE(fs.plan_tracking_cnt, 0::bigint) AS "加强跟踪",
    COALESCE(hs.plan_repair_cnt, 0::bigint) AS "计划维修",
        CASE
            WHEN COALESCE(fs.immediate_repair_cnt, 0::bigint) = 0 AND COALESCE(fs.plan_tracking_cnt, 0::bigint) = 0 AND COALESCE(hs.plan_repair_cnt, 0::bigint) = 0 THEN 1
            ELSE 0
        END AS "正常运营"
   FROM unique_trains ut
     LEFT JOIN latest_conditions lc ON ut.dvc_train_no = lc.dvc_train_no AND lc.rn = 1
     LEFT JOIN fault_stats fs ON ut.dvc_train_no = fs.dvc_train_no
     LEFT JOIN health_stats hs ON ut.dvc_train_no = hs.dvc_train_no;


