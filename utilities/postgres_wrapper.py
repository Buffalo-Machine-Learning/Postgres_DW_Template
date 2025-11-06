import yaml
import psycopg2
from psycopg2 import sql
import pandas as pd
from typing import Any, List
import io
import uuid
from datetime import datetime, timezone

class Postgres:
    def __init__(self, 
        host=None, 
        port=None, 
        database=None, 
        user=None, 
        password=None,
        database_builder=False
    ):    
        
        # fill missing from config.yaml
        if not all([host, port, database, user, password]):
            with open("config.yaml", "r", encoding="utf-8") as f:
                pg = (yaml.safe_load(f) or {}).get("postgres", {})
            host     = host     or pg.get("host", "127.0.0.1")
            port     = port     or pg.get("port", 5432)
            user     = user     or pg.get("user", "postgres")
            password = password or pg.get("password", "")
            
            if database_builder:
                database = 'postgres'
            else:
                database = database or pg.get("database", "postgres")

            self.conn = psycopg2.connect(
                host=host, 
                port=port, 
                dbname=database, 
                user=user, 
                password=password
            )
            self.conn.autocommit = True  # convenient for simple reads/writes

    def __enter__(self): return self
    def __exit__(self, *exc): self.close()

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def query_df(self, sql_text: str, params=None) -> pd.DataFrame:
        with self.conn.cursor() as cur:
            cur.execute(sql_text, params or ())
            rows = cur.fetchall()
            cols = [c.name for c in cur.description]
        return pd.DataFrame(rows, columns=cols)

    def run_ddl(self, ddl_file: str):
        with open(ddl_file, "r", encoding="utf-8") as f:
            ddl_sql = f.read()
            
        with self.conn.cursor() as cur:
            cur.execute(ddl_sql)

    def _to_text(self, x: Any):
        # Convert values to textual form; None stays None (becomes SQL NULL)
        if pd.isna(x):
            return None
        return str(x)

    def truncate_table(
        self,
        schema: str,
        table: str,
        source: str = "unknown"
    ):
        err_msg = ""
        success = True
        start_time = datetime.now(timezone.utc)

        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "TRUNCATE TABLE %s.%s;",
                    (schema, table)
                )
            
            except Exception as e:
                err_msg = e
                success = False

            finally:
                self.log_ingestion(
                    source=source,
                    table=table,
                    schema=schema,
                    start_time=start_time,
                    success=success,
                    err_msg=err_msg,
                    operation="Truncate"
                )
        

    def insert_data(
        self,
        schema: str,
        table: str,
        data: pd.DataFrame,
        source: str = "unknown",
        batch_size: int = 10_000,
        operation: str = "insert"
    ):
        """
        Insert data from a DataFrame into the specified table in batches using COPY into a temp table
        and then CALL common.insert_from_temp(...) to move data into the real table and log the import.
        """
        if data is None or data.empty:
            return

        # query table columns to validate against DataFrame
        table_cols = self.query_builder(
            schema="information_schema",
            table="columns",
            columns=["column_name"],
            where="table_schema = %s AND table_name = %s",
            params=(schema, table)
        )
        
        dw_id_name = f"DW_{table.upper()}_ID"

        table_col_set = set(table_cols["column_name"].tolist())
        table_col_set -= {dw_id_name, "DATE_MODIFIED", "DATE_IN"}  # exclude DW surrogate key if present

        df_col_set = set(map(str, data.columns))

        missing_cols = table_col_set - df_col_set
        
        # drop missing columns from table_col_set
        if missing_cols:
            print(f"Warning: DataFrame is missing columns {missing_cols} required by {schema}.{table}. These will be skipped.")
            table_col_set = table_col_set - missing_cols

        data = data[[c for c in data.columns if c in table_col_set]]

        columns: List[str] = [str(c) for c in data.columns]

        # Save current autocommit state
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False  # We need transaction control
        
        start_time = datetime.now(timezone.utc)
        success = True
        err_msg = ""
        n = len(data)

        try:
            with self.conn.cursor() as cur:
                for start in range(0, n, batch_size):
                    end = min(start + batch_size, n)
                    batch = data.iloc[start:end]

                    # Start a fresh transaction for each batch
                    self.conn.rollback()  # Clean slate
                
                    try:
                        
                        
                        # If we got here, commit the transaction
                        self.conn.commit()
                        
                    except Exception as e:
                        self.conn.rollback()
                        raise e
        
        except Exception as ex:
            n = 0
            success = False
            err_msg = ex    

        finally:
            # Restore original autocommit state
            self.conn.autocommit = old_autocommit

            self.log_ingestion(
                source=source,
                schema=schema,
                table=table,
                success=success,
                err_msg=err_msg,
                operation=operation,
                insert_count=n,
                start_time=start_time
            )

        # Commit (if not autocommit)
        self.conn.commit()

    def update_data(
        self,
        schema: str,
        table: str,
        data: pd.DataFrame,
        source: str = "unknown",
        batch_size: int = 10000,
        operation: int = "update"
    ):
        """Update data in the specified table from a DataFrame in batches using update_data stored procedure."""
        if data.empty:
            return

        with self.conn.cursor() as cur:
            for start in range(0, len(data), batch_size):
                end = start + batch_size
                batch = data.iloc[start:end]
                records = batch.to_dict(orient="records")
                cur.execute(
                    sql.SQL("CALL common.update_data(%s, %s, %s, %s);"),
                    (schema, table, source, records)
                )

    def upsert_data(
        self,
        schema: str,
        table: str,
        data: pd.DataFrame,
        source: str = "unknown",
        batch_size: int = 10000
    ):
        """Upsert data in the specified table from a DataFrame in batches using upsert_data stored procedure."""
        if data.empty:
            return

        with self.conn.cursor() as cur:
            for start in range(0, len(data), batch_size):
                end = start + batch_size
                batch = data.iloc[start:end]
                
                anyarray_records = self.anyarray_from_df(batch, schema, table)

                cur.execute(
                    sql.SQL("CALL common.upsert_data(%s, %s, %s, %s);"),
                    (schema, table, source, anyarray_records)
                )

    def log_ingestion(
        self,
        source: str = "unknown",
        schema: str = "unknown",
        table: str = "unknown",
        start_time: datetime = datetime.now(timezone.utc),
        end_time: datetime = datetime.now(timezone.utc),
        insert_count: int = 0,
        update_count: int = 0,
        success: bool = True,
        err_msg: str = "",
        operation: str = 0
        ):
        
        with self.conn.cursor() as cur:
            cur.execute("CALL common.log_ingestion(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                (
                    source, schema, table,
                    start_time, end_time,
                    insert_count, update_count, success, err_msg, operation
                )
            )

    def get_max_value(self, schema: str, table: str, field: str):
        df = self.query_builder(
            schema=schema,
            table=table,
            aggregates={ "max_value": ("max", field) }
        )

        if pd.api.types.is_integer_dtype(df["max_value"]):
            return int(df.at[0, "max_value"]) if not df.empty else None
        elif pd.api.types.is_float_dtype(df["max_value"]):
            return float(df.at[0, "max_value"]) if not df.empty else None

        return df.at[0, "max_value"] if not df.empty else None

    def query_builder(
        self,
        sql_text: str | None = None, *,
        schema: str | None = None,
        table: str | None = None,
        alias: str | None = None,
        columns="*",                       # groupable columns or "*" or raw expr string or list
        distinct: bool = False,
        joins: list[dict] | None = None,
        where: str | None = None,
        group_by: list[str] | None = None, # list of expressions/cols (strings)
        having: str | None = None,         # raw SQL, use %s placeholders with params
        aggregates=None,                   # NEW: see formats below
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        ctes: dict[str, str] | None = None,
        params=None
    ) -> pd.DataFrame:
        """
        One-stop SELECT (builder or raw).
        aggregates formats accepted (any mix):
        - {"total_revenue": ("sum", "o.amount")}     # alias -> (func, identifier-or-expr)
        - [{"func":"count","expr":"*","as":"n"}]
        - [("avg","o.total","avg_total")]            # (func, expr, alias?)
        - ["sum(o.amount) AS total"]                 # raw string passthrough
        """
        if sql_text:
            return self.query_df(sql_text, params=params)
        if not table:
            raise ValueError("table is required when using builder mode")

        # ---------- helpers ----------
        def _ident(*parts):
            return sql.SQL(".").join(sql.Identifier(p) for p in parts if p)

        def _is_raw_expr(s: str) -> bool:
            # heuristic: treat as raw if it contains parentheses, operators, quotes, or spaces
            return any(ch in s for ch in " ()/*+-=<>'\"")

        def _column_list(cols):
            if cols == "*" or cols is None:
                return sql.SQL("*")
            if isinstance(cols, str):
                # treat string columns as raw expression list
                return sql.SQL(cols)
            out = []
            for c in cols:
                if isinstance(c, tuple):
                    out.append(_ident(*c))               # e.g., ("public","users","id")
                elif isinstance(c, str):
                    out.append(sql.SQL(c) if _is_raw_expr(c) else sql.Identifier(c))
                else:
                    raise TypeError(f"Unsupported column type: {type(c)}")
            return sql.SQL(", ").join(out)

        def _aggregate_list(aggs):
            """
            Build SELECT pieces for aggregates. Returns sql.Composed or sql.SQL("")
            Accepts dict, list, tuple, or raw strings (with AS).
            """
            if not aggs:
                return sql.SQL("")

            pieces = []

            # normalize to iterable of items
            items = aggs.items() if isinstance(aggs, dict) else aggs
            if isinstance(items, dict):  # unlikely since we just mapped; guard anyway
                items = items.items()

            def one(func, expr, alias=None):
                f_sql = sql.SQL(func.upper())
                # expression can be "*" (raw) or identifier/expression
                if expr == "*":
                    inner = sql.SQL("*")
                elif isinstance(expr, (tuple, list)):
                    inner = _ident(*expr)
                elif isinstance(expr, str):
                    inner = sql.SQL(expr) if _is_raw_expr(expr) else sql.Identifier(expr)
                else:
                    raise TypeError(f"Unsupported aggregate expr type: {type(expr)}")

                base = sql.SQL("{}({})").format(f_sql, inner)
                return (base if not alias else base + sql.SQL(" AS ") + sql.Identifier(alias))

            if isinstance(aggs, dict):
                # alias -> (func, expr) OR alias -> "func(expr)"
                for alias, spec in aggs.items():
                    if isinstance(spec, (tuple, list)) and len(spec) >= 2:
                        pieces.append(one(spec[0], spec[1], alias))
                    elif isinstance(spec, str):
                        pieces.append(sql.SQL(spec) if _is_raw_expr(spec) else sql.SQL(spec + f" AS {alias}"))
                    else:
                        raise ValueError(f"Bad aggregate spec for {alias}: {spec}")
            else:
                # list-like
                for spec in items:
                    if isinstance(spec, str):
                        pieces.append(sql.SQL(spec))
                    elif isinstance(spec, (tuple, list)):
                        # (func, expr[, alias])
                        if len(spec) == 2:
                            pieces.append(one(spec[0], spec[1], None))
                        elif len(spec) == 3:
                            pieces.append(one(spec[0], spec[1], spec[2]))
                        else:
                            raise ValueError(f"Bad aggregate tuple length: {spec}")
                    elif isinstance(spec, dict):
                        # {"func": "sum", "expr": "o.amount", "as": "total"}
                        pieces.append(one(spec["func"], spec["expr"], spec.get("as")))
                    else:
                        raise TypeError(f"Unsupported aggregate spec type: {type(spec)}")

            return sql.SQL(", ").join(pieces)

        # ---------- CTEs ----------
        with_sql = sql.SQL("")
        if ctes:
            cte_parts = [
                sql.SQL("{} AS ({})").format(sql.Identifier(name), sql.SQL(cte_sql))
                for name, cte_sql in ctes.items()
            ]
            with_sql = sql.SQL("WITH ") + sql.SQL(", ").join(cte_parts) + sql.SQL(" ")

        # ---------- SELECT ----------
        select_head = sql.SQL("SELECT ") + (sql.SQL("DISTINCT ") if distinct else sql.SQL(""))

        # split SELECT into non-agg columns and aggregates (if any)
        select_cols = _column_list(columns)
        agg_cols = _aggregate_list(aggregates)

        if agg_cols.as_string(self.conn) if hasattr(agg_cols, "as_string") else str(agg_cols):
            select_list = (
                select_cols if (columns not in (None, "*")) else sql.SQL("")
            )
            # add comma if both present
            if select_list.as_string(self.conn) if hasattr(select_list, "as_string") else str(select_list):
                select_list = select_list + sql.SQL(", ") + agg_cols
            else:
                select_list = agg_cols
        else:
            select_list = select_cols

        # ---------- FROM / JOIN ----------
        from_core = _ident(schema, table)
        from_sql = (
            sql.SQL(" FROM {} AS {}").format(from_core, sql.Identifier(alias))
            if alias else
            sql.SQL(" FROM {}").format(from_core)
        )

        join_sql = sql.SQL("")
        if joins:
            chunks = []
            for j in joins:
                jtype   = (j.get("type") or "INNER").upper()
                jschema = j.get("schema")
                jtable  = j["table"]
                jalias  = j.get("alias")
                jon     = j["on"]

                jfrom = _ident(jschema, jtable)
                if jalias:
                    chunks.append(
                        sql.SQL(" {} JOIN {} AS {} ON ").format(
                            sql.SQL(jtype), jfrom, sql.Identifier(jalias)
                        ) + sql.SQL(jon)
                    )
                else:
                    chunks.append(
                        sql.SQL(" {} JOIN {} ON ").format(
                            sql.SQL(jtype), jfrom
                        ) + sql.SQL(jon)
                    )
            join_sql = sql.Composed(chunks)

        # ---------- WHERE / GROUP / HAVING / ORDER / LIMIT / OFFSET ----------
        where_sql  = sql.SQL(" WHERE ") + sql.SQL(where) if where else sql.SQL("")

        group_sql  = sql.SQL("")
        if group_by:
            group_sql = sql.SQL(" GROUP BY ") + sql.SQL(", ").join(sql.SQL(x) for x in group_by)

        having_sql = sql.SQL(" HAVING ") + sql.SQL(having) if having else sql.SQL("")

        order_sql  = sql.SQL("")
        if order_by:
            order_sql = sql.SQL(" ORDER BY ") + sql.SQL(", ").join(sql.SQL(x) for x in order_by)

        limit_sql  = sql.SQL(" LIMIT {}").format(sql.Literal(limit)) if isinstance(limit, int) else sql.SQL("")
        offset_sql = sql.SQL(" OFFSET {}").format(sql.Literal(offset)) if isinstance(offset, int) else sql.SQL("")

        # ---------- final query ----------
        query = (
            with_sql +
            select_head + (select_list if (select_list.as_string(self.conn) if hasattr(select_list, "as_string") else str(select_list)) else sql.SQL("*")) +
            from_sql + join_sql + where_sql + group_sql + having_sql + order_sql + limit_sql + offset_sql
        )

        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            cols = [c.name for c in cur.description]
        return pd.DataFrame(rows, columns=cols)
        

