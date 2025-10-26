CREATE OR REPLACE PROCEDURE common.insert_data(
    p_schema    text,
    p_table     text,
    p_temp_table text,    -- temp table that already exists in session
    _columns    text[],   -- columns to INSERT in order
    p_source    text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start timestamptz := clock_timestamp();
    v_end   timestamptz;
    v_relid regclass := format('%I.%I', p_schema, p_table)::regclass;
    v_cols_sql text;
    v_sql text;
    v_count bigint;
BEGIN
    IF _columns IS NULL OR array_length(_columns,1) IS NULL THEN
        RAISE EXCEPTION 'Caller must provide _columns (text[]) listing target columns in the desired order';
    END IF;

    -- Build the comma-separated column list for INSERT, adding timestamp columns
    SELECT string_agg(format('%I', c), ', ')
      INTO v_cols_sql
      FROM unnest(_columns) AS t(c);

    -- Insert from temp table into target using provided column order plus timestamps
    v_sql := format(
        'INSERT INTO %s ("DATE_IN", "DATE_MODIFIED", %s) SELECT CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s FROM %I',
        v_relid,                             -- schema.table
        v_cols_sql,                          -- column list for target
        v_cols_sql,                          -- select same column names from temp table
        p_temp_table                         -- temp table identifier
    );

    EXECUTE v_sql;
    GET DIAGNOSTICS v_count = ROW_COUNT;

    v_end := clock_timestamp();

    INSERT INTO common."IngestionLog"
        ("DATE_IN","DATE_MODIFIED","SOURCE_NAME","SCHEMA_NAME","TABLE_NAME",
         "StartTime","EndTime","ImportCount","UpdateCount","Status","ErrorMessage")
    VALUES (v_start, v_end, p_source, p_schema, p_table,
            v_start, v_end, v_count, 0, TRUE, NULL);
EXCEPTION WHEN OTHERS THEN
    v_end := clock_timestamp();
    INSERT INTO common."IngestionLog"
        ("DATE_IN","DATE_MODIFIED","SOURCE_NAME","SCHEMA_NAME","TABLE_NAME",
         "StartTime","EndTime","ImportCount","UpdateCount","Status","ErrorMessage")
    VALUES (v_start, v_end, p_source, p_schema, p_table,
            v_start, v_end, 0, 0, FALSE, SQLERRM);
    RAISE;
END;
$$;