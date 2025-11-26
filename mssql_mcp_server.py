import os
from pathlib import Path

import pymssql
from dotenv import load_dotenv
from mcp.server.fastmcp import Context, FastMCP

load_dotenv(Path(__file__).with_name(".env"))


# Set env vars before starting: MSSQL_SERVER, MSSQL_DB (optional), MSSQL_USER, MSSQL_PASSWORD
def build_connection_args() -> dict:
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


server = FastMCP(name="mssql-db")


@server.tool()
async def run_query(ctx: Context, sql: str):
    """
    Execute arbitrary SQL and return result rows (for queries) or rowcount.
    """
    with pymssql.connect(**build_connection_args()) as conn:
        with conn.cursor(as_dict=True) as cur:
            cur.execute(sql)
            if cur.description:
                rows = cur.fetchall()
                columns = list(rows[0].keys()) if rows else []
                return {"columns": columns, "rows": rows}
            conn.commit()
            return {"rows_affected": cur.rowcount}


@server.tool()
async def describe_table(ctx: Context, schema: str, table_name: str):
    """
    Return column metadata for schema.table_name using INFORMATION_SCHEMA.
    """
    query = """
  SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
  ORDER BY ORDINAL_POSITION;
  """
    with pymssql.connect(**build_connection_args()) as conn:
        with conn.cursor(as_dict=True) as cur:
            cur.execute(query, (schema, table_name))
            return [
                {
                    "column": row["COLUMN_NAME"],
                    "type": row["DATA_TYPE"],
                    "nullable": row["IS_NULLABLE"] == "YES",
                    "length": row["CHARACTER_MAXIMUM_LENGTH"],
                }
                for row in cur.fetchall()
            ]


@server.tool()
async def describe_indexes_and_foreign_keys(ctx: Context, schema: str, table_name: str):
    """
    Return index definitions plus inbound/outbound foreign keys for schema.table_name.
    """
    index_query = """
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
  WHERE s.name = %s AND t.name = %s AND i.is_hypothetical = 0
  ORDER BY i.index_id, ic.index_column_id;
  """
    fk_outbound_query = """
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
  WHERE ps.name = %s AND pt.name = %s
  ORDER BY fk.name, fkc.constraint_column_id;
  """
    fk_inbound_query = """
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
  WHERE rs.name = %s AND rt.name = %s
  ORDER BY fk.name, fkc.constraint_column_id;
  """

    with pymssql.connect(**build_connection_args()) as conn:
        with conn.cursor(as_dict=True) as cur:
            cur.execute(index_query, (schema, table_name))
            index_rows = cur.fetchall()

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
            fk_outbound_rows = cur.fetchall()
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
            fk_inbound_rows = cur.fetchall()
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
