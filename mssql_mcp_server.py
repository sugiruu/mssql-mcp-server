import os
from pathlib import Path

import pymssql
from dotenv import load_dotenv
from mcp.server.fastmcp import Context, FastMCP

try:
    import pyodbc
except ImportError:  # pyodbc is optional and mainly used on Windows
    pyodbc = None

load_dotenv(Path(__file__).with_name(".env"))


def _flag_enabled(value: str | None) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}


def use_pyodbc() -> bool:
    # Default to pyodbc on Windows unless explicitly disabled.
    if _flag_enabled(os.environ.get("MSSQL_USE_PYMSSQL")):
        return False
    if _flag_enabled(os.environ.get("MSSQL_USE_PYODBC")):
        return True
    return os.name == "nt"


# Set env vars before starting: MSSQL_SERVER, MSSQL_DB (optional), MSSQL_USER, MSSQL_PASSWORD
def build_pymssql_args() -> dict:
    server = os.environ["MSSQL_SERVER"]
    database = os.environ.get("MSSQL_DB", "master")
    auth_mode = os.environ.get("MSSQL_AUTH", "sql").lower()

    args = {
        "server": server,
        "database": database,
        "charset": "UTF-8",
        "login_timeout": 5,
    }

    if auth_mode in {"windows", "trusted"}:
        # Integrated Windows authentication; uses the current OS identity.
        args["trusted"] = True
        return args

    if auth_mode != "sql":
        raise ValueError("MSSQL_AUTH must be 'sql' or 'windows'")

    user = os.environ.get("MSSQL_USER")
    password = os.environ.get("MSSQL_PASSWORD")
    if not user or not password:
        raise RuntimeError("MSSQL_USER and MSSQL_PASSWORD are required unless MSSQL_AUTH=windows")

    args["user"] = user
    args["password"] = password
    return args


def build_pyodbc_connection_string() -> str:
    if pyodbc is None:
        raise RuntimeError("pyodbc is required on Windows; install the Microsoft ODBC driver and pyodbc.")

    server = os.environ["MSSQL_SERVER"]
    port = os.environ.get("MSSQL_PORT")
    database = os.environ.get("MSSQL_DB", "master")
    auth_mode = os.environ.get("MSSQL_AUTH", "sql").lower()
    driver = os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

    if port:
        server = f"{server},{port}"

    parts = [
        f"DRIVER={{{{{driver}}}}}".format(driver=driver),
        f"SERVER={server}",
        f"DATABASE={database}",
    ]

    if auth_mode in {"windows", "trusted"}:
        parts.append("Trusted_Connection=yes")
    elif auth_mode == "sql":
        user = os.environ.get("MSSQL_USER")
        password = os.environ.get("MSSQL_PASSWORD")
        if not user or not password:
            raise RuntimeError("MSSQL_USER and MSSQL_PASSWORD are required unless MSSQL_AUTH=windows")
        parts.append(f"UID={user}")
        parts.append(f"PWD={password}")
    else:
        raise ValueError("MSSQL_AUTH must be 'sql' or 'windows'")

    # ODBC Driver 18 requires encryption to be specified; allow override if needed.
    encrypt = os.environ.get("MSSQL_ENCRYPT", "yes")
    trust_cert = os.environ.get("MSSQL_TRUST_CERT", "yes")
    parts.append(f"Encrypt={encrypt}")
    parts.append(f"TrustServerCertificate={trust_cert}")

    return ";".join(parts)


def open_connection():
    if use_pyodbc():
        conn_str = build_pyodbc_connection_string()
        return pyodbc.connect(conn_str, timeout=5)
    return pymssql.connect(**build_pymssql_args())


def param_placeholder() -> str:
    return "?" if use_pyodbc() else "%s"


def fetch_columns_and_rows(cur):
    rows = cur.fetchall()
    if not rows:
        return [], []
    first = rows[0]
    if isinstance(first, dict):
        columns = list(first.keys())
        return columns, rows
    columns = [col[0] for col in cur.description]
    dict_rows = [dict(zip(columns, row)) for row in rows]
    return columns, dict_rows


server = FastMCP(name="mssql-db")


@server.tool()
async def run_query(ctx: Context, sql: str):
    """
    Execute arbitrary SQL and return result rows (for queries) or rowcount.
    """
    with open_connection() as conn:
        cursor_args = {} if use_pyodbc() else {"as_dict": True}
        with conn.cursor(**cursor_args) as cur:
            cur.execute(sql)
            if cur.description:
                columns, rows = fetch_columns_and_rows(cur)
                return {"columns": columns, "rows": rows}
            conn.commit()
            return {"rows_affected": cur.rowcount}


@server.tool()
async def describe_table(ctx: Context, schema: str, table_name: str):
    """
    Return column metadata for schema.table_name using INFORMATION_SCHEMA.
    """
    placeholder = param_placeholder()
    query = f"""
  SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = {placeholder} AND TABLE_NAME = {placeholder}
  ORDER BY ORDINAL_POSITION;
  """
    with open_connection() as conn:
        cursor_args = {} if use_pyodbc() else {"as_dict": True}
        with conn.cursor(**cursor_args) as cur:
            cur.execute(query, (schema, table_name))
            _, rows = fetch_columns_and_rows(cur)
            return [
                {
                    "column": row["COLUMN_NAME"],
                    "type": row["DATA_TYPE"],
                    "nullable": row["IS_NULLABLE"] == "YES",
                    "length": row["CHARACTER_MAXIMUM_LENGTH"],
                }
                for row in rows
            ]


@server.tool()
async def describe_indexes_and_foreign_keys(ctx: Context, schema: str, table_name: str):
    """
    Return index definitions plus inbound/outbound foreign keys for schema.table_name.
    """
    placeholder = param_placeholder()
    index_query = f"""
  SELECT i.name AS index_name,
         i.type_desc,
         i.is_primary_key,
         i.is_unique,
         ic.is_included_column,
         ic.key_ordinal,
         c.name AS column_name
  FROM sys.indexes AS i
  INNER JOIN sys.tables t ON t.object_id = i.object_id
  INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
  INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
  INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
  WHERE s.name = {placeholder} AND t.name = {placeholder} AND i.is_hypothetical = 0
  ORDER BY i.index_id, ic.index_column_id;
  """
    fk_outbound_query = f"""
  SELECT fk.name AS constraint_name,
         pc.name AS column_name,
         rs.name AS referenced_schema,
         rt.name AS referenced_table,
         rc.name AS referenced_column
  FROM sys.foreign_keys fk
  INNER JOIN sys.tables pt ON pt.object_id = fk.parent_object_id
  INNER JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
  INNER JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
  INNER JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
  INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
  INNER JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  INNER JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE ps.name = {placeholder} AND pt.name = {placeholder}
  ORDER BY fk.name, fkc.constraint_column_id;
  """
    fk_inbound_query = f"""
  SELECT fk.name AS constraint_name,
         ps.name AS referencing_schema,
         pt.name AS referencing_table,
         pc.name AS referencing_column,
         rc.name AS referenced_column
  FROM sys.foreign_keys fk
  INNER JOIN sys.tables pt ON pt.object_id = fk.parent_object_id
  INNER JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
  INNER JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
  INNER JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
  INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
  INNER JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  INNER JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE rs.name = {placeholder} AND rt.name = {placeholder}
  ORDER BY fk.name, fkc.constraint_column_id;
  """

    with open_connection() as conn:
        cursor_args = {} if use_pyodbc() else {"as_dict": True}
        with conn.cursor(**cursor_args) as cur:
            cur.execute(index_query, (schema, table_name))
            _, index_rows = fetch_columns_and_rows(cur)

            indexes = {}
            for row in index_rows:
                idx_name = row["index_name"] or "(unnamed)"
                if idx_name not in indexes:
                    indexes[idx_name] = {
                        "name": idx_name,
                        "type": row["type_desc"],
                        "is_primary_key": bool(row["is_primary_key"]),
                        "is_unique": bool(row["is_unique"]),
                        "columns": [],
                    }
                indexes[idx_name]["columns"].append(
                    {
                        "name": row["column_name"],
                        "key_ordinal": row["key_ordinal"],
                        "is_included": bool(row["is_included_column"]),
                    }
                )

            cur.execute(fk_outbound_query, (schema, table_name))
            _, fk_outbound_rows = fetch_columns_and_rows(cur)
            outbound = {}
            for row in fk_outbound_rows:
                fk_name = row["constraint_name"]
                if fk_name not in outbound:
                    outbound[fk_name] = {
                        "name": fk_name,
                        "target_schema": row["referenced_schema"],
                        "target_table": row["referenced_table"],
                        "columns": [],
                    }
                outbound[fk_name]["columns"].append(
                    {
                        "column": row["column_name"],
                        "references": row["referenced_column"],
                    }
                )

            cur.execute(fk_inbound_query, (schema, table_name))
            _, fk_inbound_rows = fetch_columns_and_rows(cur)
            inbound = {}
            for row in fk_inbound_rows:
                fk_name = row["constraint_name"]
                if fk_name not in inbound:
                    inbound[fk_name] = {
                        "name": fk_name,
                        "source_schema": row["referencing_schema"],
                        "source_table": row["referencing_table"],
                        "columns": [],
                    }
                inbound[fk_name]["columns"].append(
                    {
                        "column": row["referencing_column"],
                        "references": row["referenced_column"],
                    }
                )

            return {
                "indexes": list(indexes.values()),
                "foreign_keys_outbound": list(outbound.values()),
                "foreign_keys_inbound": list(inbound.values()),
            }


if __name__ == "__main__":
    server.run()
