CREATE OR REPLACE VIEW public.vw_health_assessment_infomation
 AS
 SELECT dev_macda.msg_calc_dvc_no AS carriage_no,
    last(dev_macda.dwef_op_tm_u11, dev_macda.msg_calc_parse_time) AS dwef_op_tm_u11,
    last(dev_macda.dwef_op_tm_u21, dev_macda.msg_calc_parse_time) AS dwef_op_tm_u21,
    last(dev_macda.dwef_op_cnt_u11, dev_macda.msg_calc_parse_time) AS dwef_op_cnt_u11,
    last(dev_macda.dwef_op_cnt_u21, dev_macda.msg_calc_parse_time) AS dwef_op_cnt_u21,
    last(dev_macda.dwcf_op_tm_u11, dev_macda.msg_calc_parse_time) AS dwcf_op_tm_u11,
    last(dev_macda.dwcf_op_tm_u21, dev_macda.msg_calc_parse_time) AS dwcf_op_tm_u21,
    last(dev_macda.dwcf_op_cnt_u11, dev_macda.msg_calc_parse_time) AS dwcf_op_cnt_u11,
    last(dev_macda.dwcf_op_cnt_u21, dev_macda.msg_calc_parse_time) AS dwcf_op_cnt_u21,
    last(dev_macda.dwfad_op_cnt_u1, dev_macda.msg_calc_parse_time) AS dwfad_op_cnt_u1,
    last(dev_macda.dwfad_op_cnt_u2, dev_macda.msg_calc_parse_time) AS dwfad_op_cnt_u2,
    last(dev_macda.dwrad_op_cnt_u1, dev_macda.msg_calc_parse_time) AS dwrad_op_cnt_u1,
    last(dev_macda.dwrad_op_cnt_u2, dev_macda.msg_calc_parse_time) AS dwrad_op_cnt_u2,
    last(dev_macda.dwcp_op_tm_u11, dev_macda.msg_calc_parse_time) AS dwcp_op_tm_u11,
    last(dev_macda.dwcp_op_tm_u12, dev_macda.msg_calc_parse_time) AS dwcp_op_tm_u12,
    last(dev_macda.dwcp_op_tm_u21, dev_macda.msg_calc_parse_time) AS dwcp_op_tm_u21,
    last(dev_macda.dwcp_op_tm_u22, dev_macda.msg_calc_parse_time) AS dwcp_op_tm_u22,
    last(dev_macda.dwcp_op_cnt_u11, dev_macda.msg_calc_parse_time) AS dwcp_op_cnt_u11,
    last(dev_macda.dwcp_op_cnt_u12, dev_macda.msg_calc_parse_time) AS dwcp_op_cnt_u12,
    last(dev_macda.dwcp_op_cnt_u21, dev_macda.msg_calc_parse_time) AS dwcp_op_cnt_u21,
    last(dev_macda.dwcp_op_cnt_u22, dev_macda.msg_calc_parse_time) AS dwcp_op_cnt_u22
   FROM dev_macda
  WHERE dev_macda.msg_calc_parse_time >= (now() - '30 days'::interval)
  GROUP BY dev_macda.msg_calc_dvc_no
  ORDER BY dev_macda.msg_calc_dvc_no;