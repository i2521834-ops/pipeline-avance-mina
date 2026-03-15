create table if not exists meta.etl_runs (
    run_id bigserial primary key,
    process_name text not null,
    source_name text,
    started_at timestamptz default now(),
    ended_at timestamptz,
    status text,
    rows_read integer,
    rows_loaded integer,
    error_message text
);
