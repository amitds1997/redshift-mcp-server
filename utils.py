import regex
import sqlglot
from sqlalchemy import literal
from sqlalchemy.dialects import postgresql
from sqlglot.expressions import CTE, Command, Describe, Select, Subquery, Values

from constants import SUSPICIOUS_QUERY_REGEXP


def is_read_only_expression(exp: sqlglot.Expression) -> bool:
    top_level_check = isinstance(exp, (Select, Describe)) or (
        isinstance(exp, Command)
        and exp.this
        and exp.this.upper() in {"EXPLAIN", "SHOW"}
    )

    if not top_level_check:
        return False

    for node in exp.walk():
        if isinstance(node, CTE):
            if not isinstance(node.this, (Select, Values)):
                return False

        if isinstance(node, Subquery):
            if not isinstance(node.this, (Select, Values)):
                return False

    return True


def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Validate if the provided SQL statement is read-only as per Redshift standards.

    Args:
        sql (str): The SQL statement to validate.

    Returns:
        tuple[bool, str]: A tuple where the first element is a boolean indicating if the SQL is read-only, and the second element is error message
    """
    if not sql.strip():
        return False, "SQL statement cannot be empty or whitespace."

    processed_sql = sqlglot.parse(sql, read="redshift")

    if len(processed_sql) > 1:
        return False, "Only one SQL statement is allowed at a time."

    msg = "Not a valid SQL statement. Only SELECT, DESCRIBE, SHOW and EXPLAIN statements are allowed."
    if processed_sql[0] is None or not is_read_only_expression(processed_sql[0]):
        return False, f"{msg}\nInvalid SQL statement: {sql}"

    return True, "This is a read-only SQL statement."


def quote_sql_literal(value) -> str:
    """Safely quote a SQL literal value."""
    return str(
        literal(value).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def check_for_suspicious_sql(sql: str) -> bool:
    if regex.compile(SUSPICIOUS_QUERY_REGEXP).search(sql):
        raise Exception(f"SQL contains suspicious patterns, execution rejected: {sql}")
    return True
