CREATE OR REPLACE VIEW public.vw_traintemperature
 AS
 SELECT max(pro_macda.msg_calc_train_no) AS train_no,
    pro_macda.msg_calc_dvc_no AS carriage_no,
    last(pro_macda.ras_sys, pro_macda.msg_calc_dvc_time) AS ras_sys,
    last(pro_macda.tic, pro_macda.msg_calc_dvc_time) AS tic,
    last(pro_macda.fas_sys, pro_macda.msg_calc_dvc_time) AS fas_sys
   FROM pro_macda
  WHERE pro_macda.msg_calc_dvc_time >= (now() - '2 minute'::interval)
  GROUP BY pro_macda.msg_calc_dvc_no
  ORDER BY pro_macda.msg_calc_dvc_no;