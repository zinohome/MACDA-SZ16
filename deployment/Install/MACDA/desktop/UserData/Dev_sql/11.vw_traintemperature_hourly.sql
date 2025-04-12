CREATE MATERIALIZED VIEW public.vw_traintemperature_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
 SELECT dev_macda.msg_calc_dvc_no AS carriage_no,
    time_bucket('01:00:00'::interval, dev_macda.msg_calc_parse_time) AS bucket,
    avg(dev_macda.ras_sys) AS ras_sys,
    avg(dev_macda.tic) AS tic,
    avg(dev_macda.fas_sys) AS fas_sys
   FROM dev_macda
   WHERE dev_macda.msg_calc_parse_time >= (now() - '3 hour'::interval)
  GROUP BY (time_bucket('01:00:00'::interval, dev_macda.msg_calc_parse_time)), dev_macda.msg_calc_dvc_no;

SELECT add_continuous_aggregate_policy('vw_traintemperature_hourly'::regclass, start_offset=>'2.5 hour'::interval, end_offset=>'1 min'::interval,  schedule_interval=>'30 min'::interval);


CREATE MATERIALIZED VIEW public.vw_traintemperature_daily
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
 SELECT dev_macda.msg_calc_dvc_no AS carriage_no,
    time_bucket('1 day'::interval, dev_macda.msg_calc_parse_time) AS bucket,
    avg(dev_macda.ras_sys) AS ras_sys,
    avg(dev_macda.tic) AS tic,
    avg(dev_macda.fas_sys) AS fas_sys
   FROM dev_macda
   WHERE dev_macda.msg_calc_parse_time >= (now() - '2.5 day'::interval)
  GROUP BY (time_bucket('1 day'::interval, dev_macda.msg_calc_parse_time)), dev_macda.msg_calc_dvc_no;

SELECT add_continuous_aggregate_policy('vw_traintemperature_daily'::regclass, start_offset=>'2.1 day'::interval, end_offset=>'1 hour'::interval,  schedule_interval=>'1 day'::interval);


CREATE OR REPLACE VIEW public.vw_traintemperature_monthly
 AS
 SELECT v.carriage_no AS carriage_no,
    time_bucket('1 mon'::interval, v.bucket) AS bucket,
    avg(v.ras_sys) AS ras_sys,
    avg(v.tic) AS tic,
    avg(v.fas_sys) AS fas_sys
   FROM vw_traintemperature_daily v
  GROUP BY (time_bucket('1 mon'::interval, v.bucket)), v.carriage_no;

