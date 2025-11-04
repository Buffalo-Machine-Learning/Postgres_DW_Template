CREATE TABLE IF NOT EXISTS common."IngestionLog"
(
    "INGESTION_LOG_ID" bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    "DATE_IN" timestamp with time zone NOT NULL DEFAULT now(),
    "SOURCE_NAME" character varying(255) NOT NULL,
    "SCHEMA_NAME" character varying(255) NOT NULL,
    "TABLE_NAME" character varying(255) NOT NULL,
    "StartTime" timestamp with time zone NOT NULL,
    "EndTime" timestamp with time zone NOT NULL,
    "TotalMillis" bigint GENERATED ALWAYS AS ( EXTRACT(EPOCH FROM ("EndTime" - "StartTime")) * 1000)::bigint),
    "ImportCount" bigint NOT NULL,
    "UpdateCount" bigint NOT NULL,
    "Status" BOOLEAN NOT NULL,
    "ErrorMessage" text,
    "Type" character varying(50) NOT NULL,
    CONSTRAINT "IngestionLog_pkey" PRIMARY KEY ("INGESTION_LOG_ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."IngestionLog"
    OWNER to postgres;