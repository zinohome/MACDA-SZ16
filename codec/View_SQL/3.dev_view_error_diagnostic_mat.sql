CREATE VIEW dev_view_error_diagnostic_mat AS
SELECT
    et.msg_calc_dvc_no,
    et.msg_calc_train_no,
    et.dvc_train_no,
    et.dvc_carriage_no,
    et.param_name,
    et.error_start_time,
    et.error_end_time,
    et.error_status,
    fd.fault_level,
    fd.repair_suggestion
FROM
    dev_view_error_timed_mat et
LEFT JOIN
    fault_diagnostic fd
ON
    et.param_name = fd.fault_name;
