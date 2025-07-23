import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from dotenv import load_dotenv
from loguru import logger
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from constants import (
    SVV_ALL_COLUMNS_QUERY,
    SVV_ALL_SCHEMAS_QUERY,
    SVV_ALL_TABLES_QUERY,
    SVV_REDSHIFT_DATABASES_QUERY,
)
from db_utils import RedshiftDB
from models import (
    RedshiftColumn,
    RedshiftDatabase,
    RedshiftSchema,
    RedshiftTable,
)
from utils import check_for_suspicious_sql, quote_sql_literal, validate_sql

load_dotenv()


logger.info("Logging initialized for Redshift MCP Server")

MAX_REQUEST_RUNTIME_MINUTES = 30


@dataclass
class AppContext:
    db: RedshiftDB


@asynccontextmanager
async def app_lifespan(_: FastMCP) -> AsyncIterator[AppContext]:
    redshift_db = RedshiftDB.connect()
    try:
        yield AppContext(redshift_db)
    finally:
        redshift_db.disconnect()


server = FastMCP("Redshift MCP Server", lifespan=app_lifespan)


@server.tool(
    name="list_databases",
    description="Fetch all databases present in the Redshift cluster",
)
async def list_database_tool(ctx: Context) -> list[RedshiftDatabase]:
    """List all databases in the Redshift cluster

    This tool queries SVV_REDSHIFT_DATABASES to retrieve all database information that the user has access to.

    ## Response structure

    Returns a list of `RedshiftDatabase` objects, each containing:

    - database_name: The name of the database.
    - database_owner: The user ID of the database owner.
    - database_type: The type of the database (e.g., 'local').
    - database_acl: The permissions for the specified user or user group for the database.
    - database_options: Properties of the database
    - database_isolation_level: The isolation level of the database (e.g., Snapshot Isolation vs Serializable).

    ## Interpretation Best Practices

    1. Focus on 'local' database types for cluster-native databases.
    2. 'shared' database types indicate databases shared via datashares.
    3. Use database names for subsequent schema and table discovery.
    4. Note the isolation level for transaction planning.
    """
    db: RedshiftDB = ctx.request_context.lifespan_context.db

    try:
        logger.info(f"Listing databases in the Redshift cluster {db.host}")
        database_data = await db.run_query(SVV_REDSHIFT_DATABASES_QUERY)

        databases = [
            RedshiftDatabase.model_validate(db)
            for db in database_data.to_dict(orient="records")
        ]

        logger.info(f"Successfully retrieved {len(databases)} databases from cluster")
        return databases
    except Exception as e:
        logger.error(f"Error in list_databases_tool: {str(e)}")
        await ctx.error(f"Failed to list databases on cluster {db.host}: {str(e)}")
        raise


@server.tool(
    name="list_schemas",
    description="Fetch all Redshift schemas in database and return it in structured format",
)
async def list_schemas_tool(
    ctx: Context,
    db_name: str = Field(
        ...,
        description="The database name to list schemas for. Must be a valid database name from the list_databases tool.",
    ),
) -> list[RedshiftSchema]:
    """List all schemas in a specified database within the Redshift cluster

    This tool queries SVV_ALL_SCHEMAS to retrieve all schema information that the user has access to in the specified database.

    ## Parameters

    - db_name (str): The name of the database to list schemas for. IMPORTANT: Must be a valid database name from the list_databases tool.

    ## Response structure

    Returns a list of `RedshiftSchema` objects, each containing:

    - database_name: The name of the database where the schema exists.
    - schema_name: The name of the schema.
    - schema_owner: The user ID of the schema owner.
    - schema_type: The type of the schema (external, local, or shared).
    - schema_acl: The permissions for the specified user or user group for the schema.
    - source_database: The name of the source database for external schema.
    - schema_option: The options of the schema (external schema attribute).

    ## Usage Tips

    1. First use list_clusters to get valid cluster identifiers.
    2. Then use list_databases to get valid database names for the cluster.
    3. Ensure the cluster status is 'available' before querying schemas.
    4. Note schema types to understand if they are local, external, or shared.
    5. External schemas connect to external data sources like S3 or other databases.

    ## Interpretation Best Practices

    1. Focus on 'local' schema types for cluster-native schemas.
    2. 'external' schema types indicate connections to external data sources.
    3. 'shared' schema types indicate schemas from datashares.
    4. Use schema names for subsequent table and column discovery.
    5. Consider schema permissions (schema_acl) for access planning.
    """
    db: RedshiftDB = ctx.request_context.lifespan_context.db

    try:
        logger.info(f"Listing schemas for database: {db_name} in cluster {db.host}")
        schema_data = await db.run_query(
            SVV_ALL_SCHEMAS_QUERY.format(quote_sql_literal(db_name))
        )

        schemas = [
            RedshiftSchema.model_validate(schema)
            for schema in schema_data.to_dict(orient="records")
        ]

        logger.info(
            f"Successfully retrieved {len(schemas)} schemas for database {db_name}"
        )
        return schemas
    except Exception as e:
        logger.error(f"Error listing schemas for database {db_name}: {e}")
        await ctx.error(
            f"Failed to list schemas for database {db_name} on cluster {db.host}: {str(e)}"
        )
        raise


@server.tool(
    name="list_tables",
    description="Fetch all Redshift tables in a schema and return it in structured format",
)
async def list_tables_tool(
    ctx: Context,
    db_name: str = Field(
        ...,
        description="The database name to list tables for. Must be a valid database name from the list_databases tool.",
    ),
    schema_name: str = Field(
        ...,
        description="The schema name to list tables for. Must be a valid schema name from the list_schemas tool.",
    ),
) -> list[RedshiftTable]:
    """List all tables in a specified schema within the Redshift cluster

    This tool queries SVV_ALL_TABLES to retrieve all table information that the user has access to in the specified database and schema.

    ## Parameters

    - db_name (str): The name of the database to list tables for. Must be a valid database name from the list_databases tool.
    - schema_name (str): The name of the schema to list tables for. Must be a valid schema name from the list_schemas tool.

    ## Response structure

    Returns a list of `RedshiftTable` objects, each containing:

    - database_name: The name of the database where the table exists.
    - schema_name: The name of the schema where the table exists.
    - table_name: The name of the table.
    - table_acl: The permissions for the specified user or user group for the table.
    - table_type: The type of the table (e.g., base, view, external).
    - remarks: Additional remarks or comments about the table.

    ## Usage Tips

    1. First use list_databases to get valid database names for the cluster.
    2. Then use list_schemas to get valid schema names for the database.
    3. Ensure the database and schema exist before querying tables.
    4. Note table types to understand if they are base tables, views, or external tables.

    ## Interpretation Best Practices

    1. Focus on 'TABLE' table types for regular database tables.
    2. 'VIEW' table types indicate views, which are virtual tables based on queries.
    3. 'EXTERNAL TABLE' types indicate tables that reference external data sources.
    4. 'SHARED TABLE' types indicate tables shared via datashares.
    5. Use table names for subsequent column discovery.
    6. Consider table permissions (table_acl) for access planning.
    """
    db: RedshiftDB = ctx.request_context.lifespan_context.db

    try:
        logger.info(
            f"Listing tables for database: {db_name}, schema: {schema_name} in cluster {db.host}"
        )
        table_data = await db.run_query(
            SVV_ALL_TABLES_QUERY.format(
                quote_sql_literal(db_name), quote_sql_literal(schema_name)
            )
        )

        tables = [
            RedshiftTable.model_validate(table)
            for table in table_data.to_dict(orient="records")
        ]

        logger.info(
            f"Successfully retrieved {len(tables)} tables for database {db_name}, schema {schema_name}"
        )
        return tables
    except Exception as e:
        logger.error(
            f"Error listing tables for database {db_name}, schema {schema_name}: {e}"
        )
        await ctx.error(
            f"Failed to list tables for database {db_name}, schema {schema_name} on cluster {db.host}: {str(e)}"
        )
        raise


@server.tool(
    name="list_columns",
    description="Fetch all columns in a table within a schema in the Redshift cluster",
)
async def list_columns_tool(
    ctx: Context,
    db_name: str = Field(
        ...,
        description="The database name to list columns for. Must be a valid database name from the list_databases tool.",
    ),
    schema_name: str = Field(
        ...,
        description="The schema name to list columns for. Must be a valid schema name from the list_schemas tool.",
    ),
    table_name: str = Field(
        ...,
        description="The table name to list columns for. Must be a valid table name from the list_tables tool.",
    ),
) -> list[RedshiftColumn]:
    """List all columns in a specified table within a schema in the Redshift cluster

    This tool queries SVV_COLUMNS to retrieve all column information that the user has access to in the specified database, schema, and table.

    ## Parameters

    - db_name (str): The name of the database to list columns for. Must be a valid database name from the list_databases tool.
    - schema_name (str): The name of the schema to list columns for. Must be a valid schema name from the list_schemas tool.
    - table_name (str): The name of the table to list columns for. Must be a valid table name from the list_tables tool.

    ## Response structure

    Returns a list of `RedshiftColumn` objects, each containing:

    - database_name: The name of the database where the table exists.
    - schema_name: The name of the schema where the table exists.
    - table_name: The name of the table where the column exists.
    - column_name: The name of the column.
    - ordinal_position: The position of the column in the table.
    - column_default: The default value for the column, if any.
    - is_nullable: Indicates if the column can contain NULL values.
    - data_type: The data type of the column (e.g., integer, varchar).
    - character_maximum_length: The maximum length for character types.
    - numeric_precision: The precision for numeric types.
    - numeric_scale: The scale for numeric types.
    - remarks: Additional remarks or comments about the column.

    ## Usage Tips

    1. First use list_databases to get valid database names for the cluster.
    2. Then use list_schemas to get valid schema names for the database.
    3. Then use list_tables to get valid table names for the schema.
    4. Ensure the database, schema, and table exist before querying columns.
    5. Note data types and constraints for query planning and data validation.

    ## Interpretation Best Practices

    1. Use ordinal_position to understand column order in the table.
    2. Check is_nullable to understand if the column can contain NULL values.
    3. Use data_type to understand the type of data stored in the column.
    4. Consider character_maximum_length for character types and numeric_precision/scale for numeric types.
    5. Use column names for subsequent query construction and data manipulation.
    6. Consider column remarks for additional context or documentation.
    """
    db: RedshiftDB = ctx.request_context.lifespan_context.db

    try:
        logger.info(
            f"Listing columns for database: {db_name}, schema: {schema_name}, table: {table_name} in cluster {db.host}"
        )
        columns_data_df = await db.run_query(
            SVV_ALL_COLUMNS_QUERY.format(
                quote_sql_literal(db_name),
                quote_sql_literal(schema_name),
                quote_sql_literal(table_name),
            )
        )
        # `numeric_precision` and `numeric_scale` can be NaN and pandas will infer the column type as float when it should be int
        for column in [
            "numeric_precision",
            "numeric_scale",
            "character_maximum_length",
        ]:
            columns_data_df[column] = columns_data_df[column].astype("Int64")

        columns = [
            RedshiftColumn.model_validate(column)
            for column in columns_data_df.to_dict(orient="records")
        ]

        logger.info(
            f"Successfully retrieved {len(columns)} columns for database {db_name}, schema {schema_name}, table {table_name}"
        )
        return columns
    except Exception as e:
        logger.error(
            f"Error listing columns for database {db_name}, schema {schema_name}, table {table_name}: {e}"
        )
        await ctx.error(
            f"Failed to list columns for database {db_name}, schema {schema_name}, table {table_name} on cluster {db.host}: {str(e)}"
        )
        raise


@server.tool(
    name="execute_sql_tool",
    description="Execute a read-only SQL statement and return a pretty-printed result",
)
async def execute_sql_tool(
    ctx: Context,
    sql: str = Field(
        ...,
        description="The SQL statement to execute. Must be a valid single read-only SQL statement.",
    ),
) -> str:
    """Execute a read-only SQL statement and return a pretty-printed result

    This tool validates and executes a read-only SQL statement, such as SELECT, EXPLAIN, SHOW, or DESCRIBE.

    ## Parameters

    - sql (str): The SQL statement to execute. Must be a valid single read-only SQL statement.

    ## Response structure

    Returns a string containing the pretty-printed result of the SQL execution in Markdown format.

    ## Usage Tips

    1. Ensure the SQL statement is read-only (e.g., SELECT, EXPLAIN, SHOW, DESCRIBE).
    2. Use this tool for data retrieval or schema exploration.
    3. Avoid using DML or DDL statements as they are not supported.
    4. Use LIMIT clauses to restrict result sizes for large datasets.
    5. Consider using metadata discovery tools to explore databases, schemas, tables, and columns before executing complex queries.
    """
    db: RedshiftDB = ctx.request_context.lifespan_context.db

    # Validate the SQL statement
    is_read_only, validation_msg = validate_sql(sql)
    if not is_read_only:
        await ctx.error(validation_msg)
        raise ValueError(validation_msg)

    try:
        # Check for suspicious patterns
        check_for_suspicious_sql(sql)
        logger.info(f"Executing SQL statement: {sql}")

        start_time = time.time()
        result_df = await db.run_query(sql)
        execution_time_ms = int((time.time() - start_time) * 1000)

        logger.info(f"SQL statement executed successfully in {execution_time_ms} ms")
        return f"""
        {result_df.to_markdown(index=False, tablefmt="github")}

        Follow the user's instructions to interpret the results. You want to provide a clear and concise summary of the results, if nothing else is specified.
        """
    except Exception as e:
        logger.error(f"Error executing SQL statement: {e}")
        await ctx.error(f"Failed to execute SQL statement: {str(e)}")
        raise


if __name__ == "__main__":
    server.run(transport="stdio")