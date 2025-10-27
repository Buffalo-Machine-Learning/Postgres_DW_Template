CREATE OR REPLACE PROCEDURE common.update_data(
    p_schema      text,
    p_table       text,
    p_temp_table  text,      -- temp/staging table that already exists in session
    p_columns     text[],    -- columns available in temp & target (order not critical here)
    p_key_columns text[],    -- columns to join on (must exist in both tables)
    p_source      text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start       timestamptz := clock_timestamp();
    v_end         timestamptz;
    v_relid       regclass := format('%I.%I', p_schema, p_table)::regclass;
    v_count       bigint := 0;

    v_update_cols text[];    -- p_columns minus keys and timestamp columns
    v_set_sql     text;      -- SET list: col1 = t.col1, col2 = t.col2, ...
    v_join_sql    text;      -- join predicate on key columns
    v_sql         text;
BEGIN
    IF p_columns IS NULL OR array_length(p_columns, 1) IS NULL THEN
        RAISE EXCEPTION 'Caller must provide p_columns (text[])';
    END IF;
    IF p_key_columns IS NULL OR array_length(p_key_columns, 1) IS NULL THEN
        RAISE EXCEPTION 'Caller must provide p_key_columns (text[])';
    END IF;

    -- Derive update column set = p_columns \ key_columns \ {DATE_IN, DATE_MODIFIED}
    SELECT array_agg(c)
      INTO v_update_cols
      FROM unnest(p_columns) AS c
     WHERE NOT (c = ANY (p_key_columns))
       AND lower(c) NOT IN ('date_in','date_modified');

    -- Build SET list (may be empty; we'll still stamp DATE_MODIFIED)
    IF v_update_cols IS NOT NULL AND array_length(v_update_cols,1) > 0 THEN
        SELECT string_agg(format('%1$I = t.%1$I', c), ', ')
          INTO v_set_sql
          FROM unnest(v_update_cols) AS c;
    ELSE
        v_set_sql := NULL;
    END IF;

    -- Build join predicate on keys
    SELECT string_agg(format('d.%1$I = t.%1$I', k), ' AND ')
      INTO v_join_sql
      FROM unnest(p_key_columns) AS k;

    IF v_join_sql IS NULL THEN
        RAISE EXCEPTION 'Join predicate is empty; check p_key_columns';
    END IF;

    -- UPDATE target FROM temp table; always bump DATE_MODIFIED
    v_sql := format(
        'UPDATE %s AS d
           SET "DATE_MODIFIED" = CURRENT_TIMESTAMP%s%s
          FROM %I AS t
         WHERE %s',
        v_relid,
        CASE WHEN v_set_sql IS NOT NULL THEN ', ' ELSE '' END,
        COALESCE(v_set_sql, ''),
        p_temp_table,
        v_join_sql
    );

    EXECUTE v_sql;
    GET DIAGNOSTICS v_count = ROW_COUNT;

    v_end := clock_timestamp();

    INSERT INTO common."IngestionLog"
        ("DATE_IN","DATE_MODIFIED","SOURCE_NAME","SCHEMA_NAME","TABLE_NAME",
         "StartTime","EndTime","ImportCount","UpdateCount","Status","ErrorMessage","Type")
    VALUES (v_start, v_end, p_source, p_schema, p_table,
            v_start, v_end, 0, v_count, TRUE, NULL, 'UPDATE');

EXCEPTION WHEN OTHERS THEN
    v_end := clock_timestamp();
    INSERT INTO common."IngestionLog"
        ("DATE_IN","DATE_MODIFIED","SOURCE_NAME","SCHEMA_NAME","TABLE_NAME",
         "StartTime","EndTime","ImportCount","UpdateCount","Status","ErrorMessage","Type")
    VALUES (v_start, v_end, p_source, p_schema, p_table,
            v_start, v_end, 0, 0, FALSE, SQLERRM, 'UPDATE');
    RAISE;
END;
$$;