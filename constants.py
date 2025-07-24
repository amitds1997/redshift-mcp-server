SVV_REDSHIFT_DATABASES_QUERY = """
SELECT
    database_name,
    database_owner,
    database_type,
    database_acl,
    database_options,
    database_isolation_level
FROM pg_catalog.svv_redshift_databases
ORDER BY database_name;
"""

SVV_ALL_SCHEMAS_QUERY = """
SELECT
    database_name,
    schema_name,
    schema_owner,
    schema_type,
    schema_acl,
    source_database,
    schema_option
FROM pg_catalog.svv_all_schemas
WHERE database_name = {}
ORDER BY schema_name;
"""

SVV_ALL_TABLES_QUERY = """
SELECT
    database_name,
    schema_name,
    table_name,
    table_acl,
    table_type,
    remarks
FROM pg_catalog.svv_all_tables
WHERE database_name = {} AND schema_name = {}
ORDER BY table_name;
"""

SVV_ALL_COLUMNS_QUERY = """
SELECT
    database_name,
    schema_name,
    table_name,
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    remarks
FROM pg_catalog.svv_all_columns
WHERE database_name = {} AND schema_name = {} AND table_name = {}
ORDER BY ordinal_position;
"""
# SQL guardrails

# Single-lines comments.
re_slc = r"--.*?$"


def re_mlc(g: int) -> str:
    """Multi-line comments, considering balanced recursion."""
    return rf"(?P<mlc{g}>(?:\/\*)(?:[^\/\*]|\/[^\*]|\*[^\/]|(?P>mlc{g}))*(?:\*\/))"


def re_sp(g: int) -> str:
    """Whitespaces, comments, semicolons which can occur between words."""
    return rf"({re_slc}|{re_mlc(g)}|\s|;)"


# We consider `(END|COMMIT|ROLLBACK|ABORT) [WORK|TRANSACTION]` as a breaker for the `BEGIN READ ONLY; {sql}; END;`
# guarding wrapper, having there might be variations of whitespaces and comments in the construct.
SUSPICIOUS_QUERY_REGEXP = rf"(?im)(^|;){re_sp(1)}*(END|COMMIT|ROLLBACK|ABORT)({re_sp(2)}+(WORK|TRANSACTION))?{re_sp(3)}*;"
