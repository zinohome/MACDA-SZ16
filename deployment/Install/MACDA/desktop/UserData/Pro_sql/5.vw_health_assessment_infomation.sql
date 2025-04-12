CREATE OR REPLACE VIEW public.vw_health_assessment_infomation
 AS
 SELECT pro_macda.msg_calc_dvc_no AS carriage_no,
    last(pro_macda.dwef_op_tm_u11, pro_macda.msg_calc_dvc_time) AS dwef_op_tm_u11,
    last(pro_macda.dwef_op_tm_u21, pro_macda.msg_calc_dvc_time) AS dwef_op_tm_u21,
    last(pro_macda.dwef_op_cnt_u11, pro_macda.msg_calc_dvc_time) AS dwef_op_cnt_u11,
    last(pro_macda.dwef_op_cnt_u21, pro_macda.msg_calc_dvc_time) AS dwef_op_cnt_u21,
    last(pro_macda.dwcf_op_tm_u11, pro_macda.msg_calc_dvc_time) AS dwcf_op_tm_u11,
    last(pro_macda.dwcf_op_tm_u21, pro_macda.msg_calc_dvc_time) AS dwcf_op_tm_u21,
    last(pro_macda.dwcf_op_cnt_u11, pro_macda.msg_calc_dvc_time) AS dwcf_op_cnt_u11,
    last(pro_macda.dwcf_op_cnt_u21, pro_macda.msg_calc_dvc_time) AS dwcf_op_cnt_u21,
    last(pro_macda.dwfad_op_cnt_u1, pro_macda.msg_calc_dvc_time) AS dwfad_op_cnt_u1,
    last(pro_macda.dwfad_op_cnt_u2, pro_macda.msg_calc_dvc_time) AS dwfad_op_cnt_u2,
    last(pro_macda.dwrad_op_cnt_u1, pro_macda.msg_calc_dvc_time) AS dwrad_op_cnt_u1,
    last(pro_macda.dwrad_op_cnt_u2, pro_macda.msg_calc_dvc_time) AS dwrad_op_cnt_u2,
    last(pro_macda.dwcp_op_tm_u11, pro_macda.msg_calc_dvc_time) AS dwcp_op_tm_u11,
    last(pro_macda.dwcp_op_tm_u12, pro_macda.msg_calc_dvc_time) AS dwcp_op_tm_u12,
    last(pro_macda.dwcp_op_tm_u21, pro_macda.msg_calc_dvc_time) AS dwcp_op_tm_u21,
    last(pro_macda.dwcp_op_tm_u22, pro_macda.msg_calc_dvc_time) AS dwcp_op_tm_u22,
    last(pro_macda.dwcp_op_cnt_u11, pro_macda.msg_calc_dvc_time) AS dwcp_op_cnt_u11,
    last(pro_macda.dwcp_op_cnt_u12, pro_macda.msg_calc_dvc_time) AS dwcp_op_cnt_u12,
    last(pro_macda.dwcp_op_cnt_u21, pro_macda.msg_calc_dvc_time) AS dwcp_op_cnt_u21,
    last(pro_macda.dwcp_op_cnt_u22, pro_macda.msg_calc_dvc_time) AS dwcp_op_cnt_u22
   FROM pro_macda
  WHERE pro_macda.msg_calc_dvc_time >= (now() - '2 minute'::interval)
  GROUP BY pro_macda.msg_calc_dvc_no
  ORDER BY pro_macda.msg_calc_dvc_no;