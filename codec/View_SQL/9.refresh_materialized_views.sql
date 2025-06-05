\connect postgres  -- 使用postgres数据库

-- 创建刷新函数
CREATE OR REPLACE FUNCTION refresh_all_materialized_views() 
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY dev_view_error_timed_mat;
  REFRESH MATERIALIZED VIEW CONCURRENTLY pro_view_error_timed_mat;
  REFRESH MATERIALIZED VIEW CONCURRENTLY dev_view_predict_timed_mat;
  REFRESH MATERIALIZED VIEW CONCURRENTLY pro_view_predict_timed_mat;
  RETURN;
END;
$$ LANGUAGE plpgsql;

-- 执行刷新
SELECT refresh_all_materialized_views();



# 编辑cron表
crontab -e

# 添加任务（每30分钟执行一次，使用postgres数据库）
*/30 * * * * psql -U postgres -h timescaledb -d postgres -f /path/to/refresh_materialized_views.sql > /var/log/refresh_mv.log 2>&1