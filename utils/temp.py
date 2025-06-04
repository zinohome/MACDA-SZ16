create_pro_param_transposed = """
                CREATE TABLE IF NOT EXISTS pro_param_transposed (
                msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                msg_calc_parse_time TEXT NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value TEXT NULL);
            """
create_hyper_pro_param_transposed = """
                SELECT create_hypertable('pro_param_transposed', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);
            """
create_rp_pro_param_transposed = """
                SELECT add_retention_policy('pro_param_transposed', INTERVAL '1 year', if_not_exists => true);
            """
create_pro_error_transposed = """
                CREATE TABLE IF NOT EXISTS pro_error_transposed (
                msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                msg_calc_parse_time TEXT NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value TEXT NULL);
            """
create_hyper_pro_error_transposed = """
                SELECT create_hypertable('pro_error_transposed', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);
            """
create_rp_pro_error_transposed = """
                SELECT add_retention_policy('pro_error_transposed', INTERVAL '1 year', if_not_exists => true);
            """
create_pro_statistic_transposed = """
                CREATE TABLE IF NOT EXISTS pro_statistic_transposed (
                msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                msg_calc_parse_time TEXT NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value TEXT NULL);
            """
create_hyper_pro_statistic_transposed = """
                SELECT create_hypertable('pro_statistic_transposed', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);
            """
create_rp_pro_statistic_transposed = """
                SELECT add_retention_policy('pro_statistic_transposed', INTERVAL '1 year', if_not_exists => true);
            """
create_pro_predict_transposed = """
                CREATE TABLE IF NOT EXISTS pro_predict_transposed (
                msg_calc_dvc_time TIMESTAMPTZ NOT NULL,
                msg_calc_parse_time TEXT NOT NULL,
                msg_calc_dvc_no TEXT NOT NULL,
                msg_calc_train_no TEXT NOT NULL,
                dvc_train_no INTEGER NULL,
                dvc_carriage_no INTEGER NULL,
                param_name TEXT NULL,
                param_value TEXT NULL);
            """
create_hyper_pro_predict_transposed = """
                SELECT create_hypertable('pro_predict_transposed', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);
            """
create_rp_pro_predict_transposed = """
                SELECT add_retention_policy('pro_predict_transposed', INTERVAL '1 year', if_not_exists => true);
            """
