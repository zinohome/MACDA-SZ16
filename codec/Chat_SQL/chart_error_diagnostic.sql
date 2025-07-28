CREATE or replace view chart_error_diagnostic as
select
    dvc_train_no as 车号,
    dvc_carriage_no as 车厢号,
    param_name as 故障部件,
    (error_start_time AT TIME ZONE 'Asia/Shanghai')::timestamptz AS 开始时间,
    error_status as 持续状态,
    repair_suggestion as 维修建议
from public.dev_view_error_diagnostic_mat
where error_status = '持续'

union all

select
    dvc_train_no as 车号,
    dvc_carriage_no as 车厢号,
    param_name as 故障部件,
    (error_start_time AT TIME ZONE 'Asia/Shanghai')::timestamptz AS 开始时间,
    error_status as 持续状态,
    repair_suggestion as 维修建议
from public.pro_view_error_diagnostic_mat
where error_status = '持续'

order by 车号, 车厢号;
