-- Procedure to upsert data in a specified table and log the ingestion details
CREATE OR REPLACE PROCEDURE common.upsert_data(
    p_source_name VARCHAR,
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_data JSONB,
    p_key_columns TEXT[]
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

    -- Get total count before upsert
    EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE (%s) IN (SELECT %s FROM jsonb_to_recordset($1) AS x(%s))',
                   p_schema_name, p_table_name, 
                   array_to_string(p_key_columns, ','),
                   array_to_string(p_key_columns, ','),
                   p_data) 
    INTO v_update_count
    USING p_data;

    -- Upsert data into the target table
    EXECUTE format('INSERT INTO %I.%I SELECT * FROM jsonb_to_recordset($1) AS x(%s) ON CONFLICT (%s) DO UPDATE SET %s',
                   p_schema_name, p_table_name, p_data, array_to_string(p_key_columns, ','), p_data)
    USING p_data;

    v_end_time := now();
    -- Total rows in input minus updated rows equals inserted rows
    v_import_count := (SELECT COUNT(*) FROM jsonb_to_recordset(p_data) AS x) - v_update_count;

    -- Log the ingestion
    INSERT INTO common."IngestionLog" ("DATE_IN", "DATE_MODIFIED", "SOURCE_NAME", "SCHEMA_NAME", "TABLE_NAME", "StartTime", "EndTime", "ImportCount", "UpdateCount", "Status", "ErrorMessage")
    VALUES (v_start_time, v_end_time, p_source_name, p_schema_name, p_table_name, v_start_time, v_end_time, v_import_count, v_update_count, B'1', NULL);
END;
$$;
