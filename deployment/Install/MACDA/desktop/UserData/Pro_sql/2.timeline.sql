DROP TRIGGER IF EXISTS refresh_view_trigger ON public.refresh_view;
DROP FUNCTION IF EXISTS public.refresh_view_func();

create or replace function refresh_view_func() returns trigger as $$
declare
begin
    delete
    	FROM public.alarm_timeline
    	where start_time < now() - '365 day'::interval;

    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_ef_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_ef_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_ef_u11, pro_macda.msg_calc_dvc_time) as bocflt_ef_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_ef_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_ef_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_ef_u12, pro_macda.msg_calc_dvc_time) as bocflt_ef_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_cf_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_cf_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_cf_u11, pro_macda.msg_calc_dvc_time) as bocflt_cf_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_cf_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_cf_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_cf_u12, pro_macda.msg_calc_dvc_time) as bocflt_cf_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_u11, pro_macda.msg_calc_dvc_time) as bflt_vfd_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_com_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_com_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_com_u11, pro_macda.msg_calc_dvc_time) as bflt_vfd_com_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_u12, pro_macda.msg_calc_dvc_time) as bflt_vfd_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_com_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_com_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_com_u12, pro_macda.msg_calc_dvc_time) as bflt_vfd_com_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'blpflt_comp_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.blpflt_comp_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.blpflt_comp_u11, pro_macda.msg_calc_dvc_time) as blpflt_comp_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_comp_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_comp_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_comp_u11, pro_macda.msg_calc_dvc_time) as bscflt_comp_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_vent_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_vent_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_vent_u11, pro_macda.msg_calc_dvc_time) as bscflt_vent_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'blpflt_comp_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.blpflt_comp_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.blpflt_comp_u12, pro_macda.msg_calc_dvc_time) as blpflt_comp_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_comp_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_comp_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_comp_u12, pro_macda.msg_calc_dvc_time) as bscflt_comp_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_vent_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_vent_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_vent_u12, pro_macda.msg_calc_dvc_time) as bscflt_vent_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_fad_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_fad_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_fad_u11, pro_macda.msg_calc_dvc_time) as bflt_fad_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_fad_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_fad_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_fad_u12, pro_macda.msg_calc_dvc_time) as bflt_fad_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_rad_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_rad_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_rad_u11, pro_macda.msg_calc_dvc_time) as bflt_rad_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_rad_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_rad_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_rad_u12, pro_macda.msg_calc_dvc_time) as bflt_rad_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_ap_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_ap_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_ap_u11, pro_macda.msg_calc_dvc_time) as bflt_ap_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_expboard_u1' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_expboard_u1) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_expboard_u1, pro_macda.msg_calc_dvc_time) as bflt_expboard_u1
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_frstemp_u1' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_frstemp_u1) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_frstemp_u1, pro_macda.msg_calc_dvc_time) as bflt_frstemp_u1
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_rnttemp_u1' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_rnttemp_u1) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_rnttemp_u1, pro_macda.msg_calc_dvc_time) as bflt_rnttemp_u1
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_splytemp_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_splytemp_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_splytemp_u11, pro_macda.msg_calc_dvc_time) as bflt_splytemp_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_splytemp_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_splytemp_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_splytemp_u12, pro_macda.msg_calc_dvc_time) as bflt_splytemp_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_coiltemp_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_coiltemp_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_coiltemp_u11, pro_macda.msg_calc_dvc_time) as bflt_coiltemp_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_coiltemp_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_coiltemp_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_coiltemp_u12, pro_macda.msg_calc_dvc_time) as bflt_coiltemp_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_insptemp_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_insptemp_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_insptemp_u11, pro_macda.msg_calc_dvc_time) as bflt_insptemp_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_insptemp_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_insptemp_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_insptemp_u12, pro_macda.msg_calc_dvc_time) as bflt_insptemp_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_lowpres_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_lowpres_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_lowpres_u11, pro_macda.msg_calc_dvc_time) as bflt_lowpres_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_lowpres_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_lowpres_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_lowpres_u12, pro_macda.msg_calc_dvc_time) as bflt_lowpres_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_highpres_u11' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_highpres_u11) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_highpres_u11, pro_macda.msg_calc_dvc_time) as bflt_highpres_u11
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_highpres_u12' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_highpres_u12) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_highpres_u12, pro_macda.msg_calc_dvc_time) as bflt_highpres_u12
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_diffpres_u1' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_diffpres_u1) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_diffpres_u1, pro_macda.msg_calc_dvc_time) as bflt_diffpres_u1
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_ef_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_ef_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_ef_u21, pro_macda.msg_calc_dvc_time) as bocflt_ef_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_ef_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_ef_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_ef_u22, pro_macda.msg_calc_dvc_time) as bocflt_ef_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_cf_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_cf_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_cf_u21, pro_macda.msg_calc_dvc_time) as bocflt_cf_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bocflt_cf_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bocflt_cf_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bocflt_cf_u22, pro_macda.msg_calc_dvc_time) as bocflt_cf_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_u21, pro_macda.msg_calc_dvc_time) as bflt_vfd_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_com_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_com_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_com_u21, pro_macda.msg_calc_dvc_time) as bflt_vfd_com_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_u22, pro_macda.msg_calc_dvc_time) as bflt_vfd_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vfd_com_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vfd_com_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vfd_com_u22, pro_macda.msg_calc_dvc_time) as bflt_vfd_com_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'blpflt_comp_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.blpflt_comp_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.blpflt_comp_u21, pro_macda.msg_calc_dvc_time) as blpflt_comp_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_comp_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_comp_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_comp_u21, pro_macda.msg_calc_dvc_time) as bscflt_comp_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_vent_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_vent_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_vent_u21, pro_macda.msg_calc_dvc_time) as bscflt_vent_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'blpflt_comp_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.blpflt_comp_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.blpflt_comp_u22, pro_macda.msg_calc_dvc_time) as blpflt_comp_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_comp_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_comp_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_comp_u22, pro_macda.msg_calc_dvc_time) as bscflt_comp_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bscflt_vent_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bscflt_vent_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bscflt_vent_u22, pro_macda.msg_calc_dvc_time) as bscflt_vent_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_fad_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_fad_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_fad_u21, pro_macda.msg_calc_dvc_time) as bflt_fad_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_fad_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_fad_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_fad_u22, pro_macda.msg_calc_dvc_time) as bflt_fad_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_rad_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_rad_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_rad_u21, pro_macda.msg_calc_dvc_time) as bflt_rad_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_rad_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_rad_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_rad_u22, pro_macda.msg_calc_dvc_time) as bflt_rad_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_ap_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_ap_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_ap_u21, pro_macda.msg_calc_dvc_time) as bflt_ap_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_expboard_u2' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_expboard_u2) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_expboard_u2, pro_macda.msg_calc_dvc_time) as bflt_expboard_u2
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_frstemp_u2' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_frstemp_u2) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_frstemp_u2, pro_macda.msg_calc_dvc_time) as bflt_frstemp_u2
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_rnttemp_u2' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_rnttemp_u2) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_rnttemp_u2, pro_macda.msg_calc_dvc_time) as bflt_rnttemp_u2
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_splytemp_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_splytemp_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_splytemp_u21, pro_macda.msg_calc_dvc_time) as bflt_splytemp_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_splytemp_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_splytemp_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_splytemp_u22, pro_macda.msg_calc_dvc_time) as bflt_splytemp_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_coiltemp_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_coiltemp_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_coiltemp_u21, pro_macda.msg_calc_dvc_time) as bflt_coiltemp_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_coiltemp_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_coiltemp_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_coiltemp_u22, pro_macda.msg_calc_dvc_time) as bflt_coiltemp_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_insptemp_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_insptemp_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_insptemp_u21, pro_macda.msg_calc_dvc_time) as bflt_insptemp_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_insptemp_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_insptemp_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_insptemp_u22, pro_macda.msg_calc_dvc_time) as bflt_insptemp_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_lowpres_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_lowpres_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_lowpres_u21, pro_macda.msg_calc_dvc_time) as bflt_lowpres_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_lowpres_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_lowpres_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_lowpres_u22, pro_macda.msg_calc_dvc_time) as bflt_lowpres_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_highpres_u21' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_highpres_u21) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_highpres_u21, pro_macda.msg_calc_dvc_time) as bflt_highpres_u21
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_highpres_u22' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_highpres_u22) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_highpres_u22, pro_macda.msg_calc_dvc_time) as bflt_highpres_u22
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_diffpres_u2' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_diffpres_u2) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_diffpres_u2, pro_macda.msg_calc_dvc_time) as bflt_diffpres_u2
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_emergivt' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_emergivt) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_emergivt, pro_macda.msg_calc_dvc_time) as bflt_emergivt
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vehtemp_u1' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vehtemp_u1) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vehtemp_u1, pro_macda.msg_calc_dvc_time) as bflt_vehtemp_u1
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_vehtemp_u2' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_vehtemp_u2) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_vehtemp_u2, pro_macda.msg_calc_dvc_time) as bflt_vehtemp_u2
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_airmon_u1' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_airmon_u1) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_airmon_u1, pro_macda.msg_calc_dvc_time) as bflt_airmon_u1
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_airmon_u2' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_airmon_u2) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_airmon_u2, pro_macda.msg_calc_dvc_time) as bflt_airmon_u2
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_currentmon' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_currentmon) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_currentmon, pro_macda.msg_calc_dvc_time) as bflt_currentmon
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_tcms' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_tcms) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_tcms, pro_macda.msg_calc_dvc_time) as bflt_tcms
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_tempover' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_tempover) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_tempover, pro_macda.msg_calc_dvc_time) as bflt_tempover
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_powersupply_u1' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_powersupply_u1) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_powersupply_u1, pro_macda.msg_calc_dvc_time) as bflt_powersupply_u1
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_powersupply_u2' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_powersupply_u2) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_powersupply_u2, pro_macda.msg_calc_dvc_time) as bflt_powersupply_u2
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_exhaustfan' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_exhaustfan) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_exhaustfan, pro_macda.msg_calc_dvc_time) as bflt_exhaustfan
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
    insert into alarm_timeline
    SELECT (toolkit_experimental.state_timeline(t.summary)).state AS state,
        (toolkit_experimental.state_timeline(t.summary)).start_time AS start_time,
        (toolkit_experimental.state_timeline(t.summary)).end_time AS end_time,
        'bflt_exhaustval' as propose
       FROM ( SELECT toolkit_experimental.timeline_agg(pro_macda.msg_calc_dvc_time, pro_macda.msg_calc_dvc_no || pro_macda.bflt_exhaustval) AS summary
                FROM (
                   select time_bucket('1 min', pro_macda.msg_calc_dvc_time) AS msg_calc_dvc_time,
                     last(pro_macda.msg_calc_dvc_no, pro_macda.msg_calc_dvc_time) as msg_calc_dvc_no,
                     last(pro_macda.bflt_exhaustval, pro_macda.msg_calc_dvc_time) as bflt_exhaustval
                     from pro_macda where pro_macda.msg_calc_dvc_time >= (now() - '1 hour'::interval) group by pro_macda.msg_calc_dvc_no, time_bucket('1 min', pro_macda.msg_calc_dvc_time)
                    ) pro_macda
              GROUP BY pro_macda.msg_calc_dvc_no) t;
  return null;
end;
$$ language plpgsql;


CREATE TRIGGER refresh_view_trigger
    AFTER UPDATE
    ON public.refresh_view
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.refresh_view_func();














