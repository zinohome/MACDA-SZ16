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