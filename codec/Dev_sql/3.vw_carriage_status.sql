CREATE OR REPLACE VIEW public.vw_carriage_status
 AS
 SELECT max(dev_macda.msg_calc_train_no) AS train_no,
    dev_macda.msg_calc_dvc_no AS carriage_no,
     max(bocflt_ef_u11) +
                max(bocflt_ef_u12) +
                max(bocflt_cf_u11) +
                max(bocflt_cf_u12) +
                max(bflt_vfd_u11) +
                max(bflt_vfd_com_u11) +
                max(bflt_vfd_u12) +
                max(bflt_vfd_com_u12) +
                max(blpflt_comp_u11) +
                max(bscflt_comp_u11) +
                max(bscflt_vent_u11) +
                max(blpflt_comp_u12) +
                max(bscflt_comp_u12) +
                max(bscflt_vent_u11) +
                max(bflt_fad_u11) +
                max(bflt_fad_u12) +
                max(bflt_rad_u11) +
                max(bflt_rad_u12) +
                max(bflt_ap_u11) +
                max(bflt_expboard_u1) +
                max(bflt_frstemp_u1) +
                max(bflt_rnttemp_u1) +
                max(bflt_splytemp_u11) +
                max(bflt_splytemp_u12) +
                max(bflt_coiltemp_u11) +
                max(bflt_coiltemp_u12) +
                max(bflt_insptemp_u11) +
                max(bflt_insptemp_u12) +
                max(bflt_lowpres_u11) +
                max(bflt_lowpres_u12) +
                max(bflt_highpres_u11) +
                max(bflt_highpres_u12) +
                max(bflt_diffpres_u1) +
                max(bocflt_ef_u21) +
                max(bocflt_ef_u22) +
                max(bocflt_cf_u21) +
                max(bocflt_cf_u22) +
                max(bflt_vfd_u21) +
                max(bflt_vfd_com_u21) +
                max(bflt_vfd_u22) +
                max(bflt_vfd_com_u22) +
                max(blpflt_comp_u21) +
                max(bscflt_comp_u21) +
                max(bscflt_vent_u21) +
                max(blpflt_comp_u22) +
                max(bscflt_comp_u22) +
                max(bscflt_vent_u21) +
                max(bflt_fad_u21) +
                max(bflt_fad_u22) +
                max(bflt_rad_u21) +
                max(bflt_rad_u22) +
                max(bflt_ap_u21) +
                max(bflt_expboard_u2) +
                max(bflt_frstemp_u2) +
                max(bflt_rnttemp_u2) +
                max(bflt_splytemp_u21) +
                max(bflt_splytemp_u22) +
                max(bflt_coiltemp_u21) +
                max(bflt_coiltemp_u22) +
                max(bflt_insptemp_u21) +
                max(bflt_insptemp_u22) +
                max(bflt_lowpres_u21) +
                max(bflt_lowpres_u22) +
                max(bflt_highpres_u21) +
                max(bflt_highpres_u22) +
                max(bflt_diffpres_u2) +
                max(bflt_emergivt) +
                max(bflt_vehtemp_u1) +
                max(bflt_vehtemp_u2) +
                max(bflt_airmon_u1) +
                max(bflt_airmon_u2) +
                max(bflt_currentmon) +
                max(bflt_tcms) +
                max(bflt_tempover) +
                max(bflt_powersupply_u1) +
                max(bflt_powersupply_u2) +
                max(bflt_exhaustfan) +
                max(bflt_exhaustval) AS alarm_count
   FROM dev_macda
  WHERE dev_macda.msg_calc_parse_time >= (now() - '30 days'::interval)
  GROUP BY dev_macda.msg_calc_dvc_no;

CREATE OR REPLACE VIEW public.vw_carriage_predict_status
 AS
 SELECT max(dev_predict.msg_calc_train_no) AS train_no,
    dev_predict.msg_calc_dvc_no AS carriage_no,
    max(dev_predict.ref_leak_u11) + max(dev_predict.ref_leak_u12) + max(dev_predict.ref_leak_u21) + max(dev_predict.ref_leak_u22) + max(dev_predict.f_cp_u1) + max(dev_predict.f_cp_u2) + max(dev_predict.f_fas) + max(dev_predict.f_ras) + max(dev_predict.cabin_overtemp) + max(dev_predict.f_presdiff_u1) + max(dev_predict.f_presdiff_u2) + max(dev_predict.f_ef_u11) + max(dev_predict.f_ef_u12) + max(dev_predict.f_ef_u21) + max(dev_predict.f_ef_u22) + max(dev_predict.f_cf_u11) + max(dev_predict.f_cf_u12) + max(dev_predict.f_cf_u21) + max(dev_predict.f_cf_u22) + max(dev_predict.f_exufan) + max(dev_predict.f_fas_u11) + max(dev_predict.f_fas_u12) + max(dev_predict.f_fas_u21) + max(dev_predict.f_fas_u22) + max(dev_predict.f_aq_u1) + max(dev_predict.f_aq_u2) AS warning_count
   FROM dev_predict
  WHERE dev_predict.msg_calc_parse_time >= (now() - '30 days'::interval)
  GROUP BY dev_predict.msg_calc_dvc_no;
