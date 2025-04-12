CREATE OR REPLACE VIEW public.vw_train_warning_info
 AS
 SELECT max(dev_predict.msg_calc_train_no) AS train_no,
    dev_predict.msg_calc_dvc_no AS carriage_no,
    last(dev_predict.msg_calc_parse_time, dev_predict.msg_calc_parse_time) AS warning_time,
    last(dev_predict.ref_leak_u11, dev_predict.msg_calc_parse_time) AS ref_leak_u11,
    last(dev_predict.ref_leak_u12, dev_predict.msg_calc_parse_time) AS ref_leak_u12,
    last(dev_predict.ref_leak_u21, dev_predict.msg_calc_parse_time) AS ref_leak_u21,
    last(dev_predict.ref_leak_u22, dev_predict.msg_calc_parse_time) AS ref_leak_u22,
    last(dev_predict.ref_pump_u1, dev_predict.msg_calc_parse_time) AS ref_pump_u1,
    last(dev_predict.ref_pump_u2, dev_predict.msg_calc_parse_time) AS ref_pump_u2,
    last(dev_predict.fat_sensor, dev_predict.msg_calc_parse_time) AS fat_sensor,
    last(dev_predict.rat_sensor, dev_predict.msg_calc_parse_time) AS rat_sensor,
    '机组1-系统1冷媒泄漏预警'::text AS ref_leak_u11_name,
    '机组1-系统2冷媒泄漏预警'::text AS ref_leak_u12_name,
    '机组2-系统1冷媒泄漏预警'::text AS ref_leak_u21_name,
    '机组2-系统2冷媒泄漏预警'::text AS ref_leak_u22_name,
    '机组1制冷（热泵）系统预警'::text AS ref_pump_u1_name,
    '机组2制冷（热泵）系统预警'::text AS ref_pump_u2_name,
    '新风温度传感器预警'::text AS fat_sensor_name,
    '回风温度传感器预警'::text AS rat_sensor_name
   FROM dev_predict
  WHERE dev_predict.msg_calc_parse_time >= (now() - '30 days'::interval) AND dev_predict.msg_calc_train_no <> '00000'::text
  GROUP BY dev_predict.msg_calc_dvc_no;