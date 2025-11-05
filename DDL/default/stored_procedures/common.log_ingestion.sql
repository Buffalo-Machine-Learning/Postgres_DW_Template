-- Base logger: insert one row
CREATE OR REPLACE PROCEDURE common.log_ingestion(
    p_source_name  text,
    p_schema_name  text,
    p_table_name   text,
    p_start        timestamptz,
    p_end          timestamptz,
    p_import_count bigint,
    p_update_count bigint,
    p_status       boolean,
    p_error_msg    text,
    p_type         varchar(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO common."IngestionLog" (
        "SOURCE_NAME","SCHEMA_NAME","TABLE_NAME",
        "StartTime","EndTime",
        "ImportCount","UpdateCount","Status","ErrorMessage","Type"
    )
    VALUES (
        p_source_name, p_schema_name, p_table_name,
        COALESCE(p_start, clock_timestamp()),
        COALESCE(p_end,   clock_timestamp()),
        COALESCE(p_import_count, 0),
        COALESCE(p_update_count, 0),
        COALESCE(p_status, FALSE),
        p_error_msg,
        COALESCE(p_type, 'UNKNOWN')
    );
END;
$$;

-- Optional convenience wrappers
CREATE OR REPLACE PROCEDURE common.log_ingestion_ok(
    p_source_name  text,
    p_schema_name  text,
    p_table_name   text,
    p_start        timestamptz,
    p_end          timestamptz,
    p_import_count bigint,
    p_update_count bigint,
    p_type         varchar(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL common.log_ingestion(
        p_source_name, p_schema_name, p_table_name,
        p_start, p_end, p_import_count, p_update_count,
        TRUE, NULL, p_type
    );
END;
$$;