CREATE TABLE IF
	NOT EXISTS alarm_timeline (
		state TEXT NOT NULL,
		start_time TIMESTAMPTZ NOT NULL,
		end_time TIMESTAMPTZ NOT NULL,
		propose TEXT
	);

create  INDEX idx_alarm_timeline ON alarm_timeline(start_time,end_time);


CREATE TABLE IF NOT EXISTS public.refresh_view
(
    refresh_view text COLLATE pg_catalog."default"
)
TABLESPACE pg_default;
