CREATE OR REPLACE VIEW public.vw_traintemperature
 AS
 SELECT max(dev_macda.msg_calc_train_no) AS train_no,
    dev_macda.msg_calc_dvc_no AS carriage_no,
    last(dev_macda.ras_sys, dev_macda.msg_calc_parse_time) AS ras_sys,
    last(dev_macda.tic, dev_macda.msg_calc_parse_time) AS tic,
    last(dev_macda.fas_sys, dev_macda.msg_calc_parse_time) AS fas_sys
   FROM dev_macda
  WHERE dev_macda.msg_calc_parse_time >= (now() - '30 days'::interval)
  GROUP BY dev_macda.msg_calc_dvc_no
  ORDER BY dev_macda.msg_calc_dvc_no;