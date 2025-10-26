CREATE OR REPLACE PROCEDURE common.truncate_table(
    p_schema_name text,
    p_table_name  text,
    p_source_name text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count bigint;
    v_start timestamptz := clock_timestamp();
    v_end   timestamptz;
    v_relid regclass;
BEGIN
    -- Resolve relation safely (handles quoted/cased identifiers)
    v_relid := format('%I.%I', p_schema_name, p_table_name)::regclass;

    -- Count rows before truncate
    EXECUTE format('SELECT COUNT(*) FROM %s', v_relid) INTO v_count;

    -- Truncate and reset identity/owned sequences to their START values
    EXECUTE format('TRUNCATE TABLE %s RESTART IDENTITY', v_relid);

    v_end := clock_timestamp();

    INSERT INTO common."IngestionLog"
        ("DATE_IN","DATE_MODIFIED","SOURCE_NAME","SCHEMA_NAME","TABLE_NAME",
         "StartTime","EndTime","ImportCount","UpdateCount","Status","ErrorMessage","Type")
    VALUES (v_start, v_end, p_source_name, p_schema_name, p_table_name,
            v_start, v_end, -v_count, 0, TRUE, NULL, 'TRUNCATE');
END;
$$;