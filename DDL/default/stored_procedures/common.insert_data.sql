-- stored proc to insert data into a specified table and update the IngestionLog

CREATE OR REPLACE PROCEDURE common.insert_data(
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_data JSONB,
    p_source_name VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP WITH TIME ZONE;
    v_end_time TIMESTAMP WITH TIME ZONE;
    v_import_count BIGINT;
    v_update_count BIGINT;
BEGIN
    v_start_time := now();

    -- Insert data into the target table
    EXECUTE format('INSERT INTO %I.%I SELECT * FROM jsonb_to_recordset($1) AS x(%s)',
                   p_schema_name, p_table_name, p_data)
    USING p_data;

    v_end_time := now();
    v_import_count := (SELECT COUNT(*) FROM jsonb_to_recordset(p_data) AS x);
    v_update_count := 0;

    -- Log the ingestion
    INSERT INTO common."IngestionLog" ("DATE_IN", "DATE_MODIFIED", "SOURCE_NAME", "SCHEMA_NAME", "TABLE_NAME", "StartTime", "EndTime", "ImportCount", "UpdateCount", "Status", "ErrorMessage")
    VALUES (v_start_time, v_end_time, p_source_name, p_schema_name, p_table_name, v_start_time, v_end_time, v_import_count, v_update_count, B'1', NULL);
END;
$$;