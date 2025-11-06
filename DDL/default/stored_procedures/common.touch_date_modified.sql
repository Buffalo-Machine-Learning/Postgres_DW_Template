CREATE SCHEMA IF NOT EXISTS common;

CREATE OR REPLACE FUNCTION common.touch_date_modified()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  -- only touch if something actually changed
  IF NEW IS DISTINCT FROM OLD THEN
    NEW."DATE_MODIFIED" := now();
  END IF;
  RETURN NEW;
END;
$$;