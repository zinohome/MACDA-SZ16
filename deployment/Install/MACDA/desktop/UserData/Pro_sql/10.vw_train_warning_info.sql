CREATE OR REPLACE VIEW public.vw_train_warning_info
 AS
 SELECT max(pro_predict.msg_calc_train_no) AS train_no,
    pro_predict.msg_calc_dvc_no AS carriage_no,
    last(pro_predict.msg_calc_dvc_time, pro_predict.msg_calc_dvc_time) AS warning_time,
    last(pro_predict.ref_leak_u11, pro_predict.msg_calc_dvc_time) AS ref_leak_u11,
    last(pro_predict.ref_leak_u12, pro_predict.msg_calc_dvc_time) AS ref_leak_u12,
    last(pro_predict.ref_leak_u21, pro_predict.msg_calc_dvc_time) AS ref_leak_u21,
    last(pro_predict.ref_leak_u22, pro_predict.msg_calc_dvc_time) AS ref_leak_u22,
    last(pro_predict.ref_pump_u1, pro_predict.msg_calc_dvc_time) AS ref_pump_u1,
    last(pro_predict.ref_pump_u2, pro_predict.msg_calc_dvc_time) AS ref_pump_u2,
    last(pro_predict.fat_sensor, pro_predict.msg_calc_dvc_time) AS fat_sensor,
    last(pro_predict.rat_sensor, pro_predict.msg_calc_dvc_time) AS rat_sensor,
    '机组1-系统1冷媒泄漏预警'::text AS ref_leak_u11_name,
    '机组1-系统2冷媒泄漏预警'::text AS ref_leak_u12_name,
    '机组2-系统1冷媒泄漏预警'::text AS ref_leak_u21_name,
    '机组2-系统2冷媒泄漏预警'::text AS ref_leak_u22_name,
    '机组1制冷（热泵）系统预警'::text AS ref_pump_u1_name,
    '机组2制冷（热泵）系统预警'::text AS ref_pump_u2_name,
    '新风温度传感器预警'::text AS fat_sensor_name,
    '回风温度传感器预警'::text AS rat_sensor_name
   FROM pro_predict
  WHERE pro_predict.msg_calc_dvc_time >= (now() - '2 minute'::interval) AND pro_predict.msg_calc_train_no <> '00000'::text
  GROUP BY pro_predict.msg_calc_dvc_no;