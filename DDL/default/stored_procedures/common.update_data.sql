-- Procedure to update data in a specified table and log the ingestion details

-- remember ingestion log table structure
-- CREATE TABLE IF NOT EXISTS common."IngestionLog"
-- (
--     "INGESTION_LOG_ID" bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
--     "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
--     "DATE_MODIFIED" timestamp with time zone NOT NULL DEFAULT now(),
--     "SOURCE_NAME" character varying(255) NOT NULL,
--     "SCHEMA_NAME" character varying(255) NOT NULL,
--     "TABLE_NAME" character varying(255) NOT NULL,
--     "StartTime" timestamp with time zone NOT NULL,
--     "EndTime" timestamp with time zone NOT NULL,
--     "ImportCount" bigint NOT NULL,
--     "UpdateCount" bigint NOT NULL,
--     "Status" bit NOT NULL,
--     "ErrorMessage" text,
--     CONSTRAINT "IngestionLog_pkey" PRIMARY KEY ("INGESTION_LOG_ID")
-- )

CREATE OR REPLACE PROCEDURE common.update_data(
    p_source_name VARCHAR,
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_data JSONB
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

    -- Update data in the target table
    EXECUTE format('UPDATE %I.%I SET %s WHERE %s',
                   p_schema_name, p_table_name, p_data)
    USING p_data;

    v_end_time := now();
    v_import_count := 0;
    v_update_count := (SELECT COUNT(*) FROM jsonb_to_recordset(p_data) AS x);

    -- Log the ingestion
    INSERT INTO common."IngestionLog" ("DATE_IN", "DATE_MODIFIED", "SOURCE_NAME", "SCHEMA_NAME", "TABLE_NAME", "StartTime", "EndTime", "ImportCount", "UpdateCount", "Status", "ErrorMessage")
    VALUES (v_start_time, v_end_time, p_source_name, p_schema_name, p_table_name, v_start_time, v_end_time, v_import_count, v_update_count, B'1', NULL);
END;
$$;