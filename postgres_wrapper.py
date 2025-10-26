import yaml
import psycopg
from psycopg import sql
import pandas as pd

class Postgres:
    def __init__(self, host=None, port=None, database=None, user=None, password=None):
        # fill missing from config.yaml
        if not all([host, port, database, user, password]):
            with open("config.yaml", "r", encoding="utf-8") as f:
                pg = (yaml.safe_load(f) or {}).get("postgres", {})
            host     = host     or pg.get("host", "127.0.0.1")
            port     = port     or pg.get("port", 5432)
            database = database or pg.get("database", "postgres")
            user     = user     or pg.get("user", "postgres")
            password = password or pg.get("password", "")

        self.conn = psycopg.connect(
            host=host, port=port, dbname=database, user=user, password=password
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

    def query_builder(
        self,
        sql_text: str | None = None, *,
        schema: str | None = None,
        table: str | None = None,
        alias: str | None = None,
        columns="*",
        distinct: bool = False,
        joins: list[dict] | None = None,
        where: str | None = None,
        group_by: list[str] | None = None,
        having: str | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        ctes: dict[str, str] | None = None,
        params=None
    ) -> pd.DataFrame:
        if sql_text:
            return self.query_df(sql_text, params=params)
        if not table:
            raise ValueError("table is required when using builder mode")

        def _ident(*parts):
            return sql.SQL(".").join(sql.Identifier(p) for p in parts if p)

        def _column_list(cols):
            if cols == "*" or cols is None:
                return sql.SQL("*")
            if isinstance(cols, str):
                return sql.SQL(cols)
            out = []
            for c in cols:
                if isinstance(c, tuple):
                    out.append(_ident(*c))
                elif isinstance(c, str):
                    out.append(sql.SQL(c))
                else:
                    raise TypeError(f"Unsupported column type: {type(c)}")
            return sql.SQL(", ").join(out)

        with_sql = sql.SQL("")
        if ctes:
            cte_parts = [
                sql.SQL("{} AS ({})").format(sql.Identifier(name), sql.SQL(cte_sql))
                for name, cte_sql in ctes.items()
            ]
            with_sql = sql.SQL("WITH ") + sql.SQL(", ").join(cte_parts) + sql.SQL(" ")

        select_head = sql.SQL("SELECT ") + (sql.SQL("DISTINCT ") if distinct else sql.SQL(""))
        select_cols = _column_list(columns)

        from_core = _ident(schema, table)
        from_sql = (
            sql.SQL(" FROM {} AS {}").format(from_core, sql.Identifier(alias))
            if alias else
            sql.SQL(" FROM {}").format(from_core)
        )

        # === JOIN block (fixed) ===
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

        query = (
            with_sql +
            select_head + select_cols +
            from_sql + join_sql + where_sql + group_sql + having_sql + order_sql + limit_sql + offset_sql
        )

        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            cols = [c.name for c in cur.description]
        return pd.DataFrame(rows, columns=cols)

    def run_ddl(self, ddl_file: str):
        with open(ddl_file, "r", encoding="utf-8") as f:
            ddl_sql = f.read()
        with self.conn.cursor() as cur:
            cur.execute(ddl_sql)

    def insert_data(
        self,
        schema: str,
        table: str,
        data: pd.DataFrame,
        source: str = "unknown",
        batch_size: int = 10000
    ):
        """Insert data from a DataFrame into the specified table in batches using insert_data stored procedure."""
        if data.empty:
            return

        with self.conn.cursor() as cur:
            for start in range(0, len(data), batch_size):
                end = start + batch_size
                batch = data.iloc[start:end]
                records = batch.to_dict(orient="records")
                cur.execute(
                    sql.SQL("CALL common.insert_data(%s, %s, %s, %s);"),
                    (schema, table, source, psycopg.Json(records))
                )

    def update_data(
        self,
        schema: str,
        table: str,
        data: pd.DataFrame,
        source: str = "unknown",
        batch_size: int = 10000
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
                    (schema, table, source, psycopg.Json(records))
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
                records = batch.to_dict(orient="records")
                cur.execute(
                    sql.SQL("CALL common.upsert_data(%s, %s, %s, %s);"),
                    (schema, table, source, psycopg.Json(records))
                )

        

