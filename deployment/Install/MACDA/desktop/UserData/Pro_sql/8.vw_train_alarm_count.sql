CREATE OR REPLACE VIEW public.vw_train_warnging_count
 AS
 SELECT pro_predict.msg_calc_train_no AS train_no,
    max(pro_predict.ref_leak_u11) + max(pro_predict.ref_leak_u12) + max(pro_predict.ref_leak_u21) + max(pro_predict.ref_leak_u22) + max(pro_predict.ref_pump_u1) + max(pro_predict.ref_pump_u2) + max(pro_predict.fat_sensor) + max(pro_predict.rat_sensor) AS warning_count
   FROM pro_predict
  WHERE pro_predict.msg_calc_dvc_time >= (now() - '2 minute'::interval) AND pro_predict.msg_calc_train_no <> '00000'::text
  GROUP BY pro_predict.msg_calc_train_no;


CREATE OR REPLACE VIEW public.vw_train_alarm_count
 AS
 SELECT t.train_no,
    sum(t.alarm_count) AS alarm_count,
    sum(t.warning_count) AS warning_count
   FROM ( SELECT max(macda.msg_calc_train_no) AS train_no,
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
            max(bflt_exhaustval) AS alarm_count,
            max(twc.warning_count) AS warning_count
           FROM pro_macda macda
             LEFT JOIN vw_train_warnging_count twc ON macda.msg_calc_train_no = twc.train_no
          WHERE macda.msg_calc_dvc_time >= (now() - '2 minute'::interval) AND macda.msg_calc_train_no <> '00000'::text
          GROUP BY macda.msg_calc_dvc_no) t
  GROUP BY t.train_no;