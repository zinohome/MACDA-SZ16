CREATE OR REPLACE VIEW public.vw_running_state_info
 AS
 SELECT max(pro_macda.msg_calc_train_no) AS train_no,
    pro_macda.msg_calc_dvc_no AS carriage_no,
    last(pro_macda.cfbk_comp_u11, pro_macda.msg_calc_dvc_time) AS cfbk_comp_u11,
    last(pro_macda.cfbk_comp_u12, pro_macda.msg_calc_dvc_time) AS cfbk_comp_u12,
    last(pro_macda.cfbk_comp_u21, pro_macda.msg_calc_dvc_time) AS cfbk_comp_u21,
    last(pro_macda.cfbk_comp_u22, pro_macda.msg_calc_dvc_time) AS cfbk_comp_u22,
    last(pro_macda.cfbk_ef_u11, pro_macda.msg_calc_dvc_time) AS cfbk_ef_u11,
    last(pro_macda.cfbk_ef_u21, pro_macda.msg_calc_dvc_time) AS cfbk_ef_u21,
    last(pro_macda.cfbk_cf_u11, pro_macda.msg_calc_dvc_time) AS cfbk_cf_u11,
    last(pro_macda.cfbk_cf_u21, pro_macda.msg_calc_dvc_time) AS cfbk_cf_u21,
    last(pro_macda.fadpos_u1, pro_macda.msg_calc_dvc_time) AS fadpos_u1,
    last(pro_macda.fadpos_u2, pro_macda.msg_calc_dvc_time) AS fadpos_u2,
    last(pro_macda.radpos_u1, pro_macda.msg_calc_dvc_time) AS radpos_u1,
    last(pro_macda.radpos_u2, pro_macda.msg_calc_dvc_time) AS radpos_u2
   FROM pro_macda
  WHERE pro_macda.msg_calc_dvc_time >= (now() - '2 minute'::interval)
  GROUP BY pro_macda.msg_calc_dvc_no
  ORDER BY pro_macda.msg_calc_dvc_no;