CREATE OR REPLACE VIEW public.vw_running_state_info
 AS
 SELECT max(dev_macda.msg_calc_train_no) AS train_no,
    dev_macda.msg_calc_dvc_no AS carriage_no,
    last(dev_macda.cfbk_comp_u11, dev_macda.msg_calc_parse_time) AS cfbk_comp_u11,
    last(dev_macda.cfbk_comp_u12, dev_macda.msg_calc_parse_time) AS cfbk_comp_u12,
    last(dev_macda.cfbk_comp_u21, dev_macda.msg_calc_parse_time) AS cfbk_comp_u21,
    last(dev_macda.cfbk_comp_u22, dev_macda.msg_calc_parse_time) AS cfbk_comp_u22,
    last(dev_macda.cfbk_ef_u11, dev_macda.msg_calc_parse_time) AS cfbk_ef_u11,
    last(dev_macda.cfbk_ef_u21, dev_macda.msg_calc_parse_time) AS cfbk_ef_u21,
    last(dev_macda.cfbk_cf_u11, dev_macda.msg_calc_parse_time) AS cfbk_cf_u11,
    last(dev_macda.cfbk_cf_u21, dev_macda.msg_calc_parse_time) AS cfbk_cf_u21,
    last(dev_macda.fadpos_u1, dev_macda.msg_calc_parse_time) AS fadpos_u1,
    last(dev_macda.fadpos_u2, dev_macda.msg_calc_parse_time) AS fadpos_u2,
    last(dev_macda.radpos_u1, dev_macda.msg_calc_parse_time) AS radpos_u1,
    last(dev_macda.radpos_u2, dev_macda.msg_calc_parse_time) AS radpos_u2
   FROM dev_macda
  WHERE dev_macda.msg_calc_parse_time >= (now() - '30 days'::interval)
  GROUP BY dev_macda.msg_calc_dvc_no
  ORDER BY dev_macda.msg_calc_dvc_no;