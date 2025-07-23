import unittest

from utils import validate_sql


class TestValidateIsReadonlySQL(unittest.TestCase):
    def test_validate_is_readonly_sql(self):
        test_cases = [
            ("SELECT * FROM users;", True),
            ("EXPLAIN SELECT * FROM users;", True),
            ("SHOW TABLES;", True),
            ("DESCRIBE users;", True),
            ("select id from table1", True),  # Check for case insensitivity
            ("eXpLaIn select 1", True),  # Check for case insensitivity
            ("SELECT 1;", True),  # Check for multiple statements
            (
                "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users;",
                True,
            ),  # CTE with only SELECTs are allowed
            (
                "WITH deleted AS (DELETE FROM users WHERE active = false RETURNING *) SELECT * FROM deleted;",
                False,
            ),  # CTE with DML is not allowed
            (
                "SELECT * FROM users WHERE id IN (SELECT user_id FROM logins);",
                True,
            ),  # Subquery in WHERE clause
            (
                "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id;",
                True,
            ),
            ("SELECT department, COUNT(*) FROM employees GROUP BY department;", True),
            (
                "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5;",
                True,
            ),
            ("SELECT id, RANK() OVER (ORDER BY created_at) FROM users;", True),
            ("SELECT * FROM logs ORDER BY timestamp DESC OFFSET 100;", True),
            ("SELECT * FROM (VALUES (1, 'A'), (2, 'B')) AS t(id, label);", True),
            (
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';",
                True,
            ),
            ("SELECT 1 -- trailing comment", True),
            ("SELECT 1 /* block comment */", True),
            ("SELECT * FROM users LIMIT 10;", True),
            ("SELECT * FROM users QUALIFY ROW_NUMBER() OVER() = 1;", True),
            ("INSERT INTO users VALUES (1, 'a');", False),
            ("UPDATE users SET name = 'b' WHERE id = 1;", False),
            ("DELETE FROM users WHERE id = 1;", False),
            ("CREATE TABLE test (id INT);", False),
            ("DROP TABLE users;", False),
            ("ALTER TABLE users ADD COLUMN age INT;", False),
            ("TRUNCATE TABLE users;", False),
            ("GRANT SELECT ON users TO user1;", False),
            ("REVOKE SELECT ON users FROM user1;", False),
            ("BEGIN;", False),
            ("COMMIT;", False),
            ("ROLLBACK;", False),
            ("VACUUM;", False),
            ("ANALYZE;", False),
            ("SET search_path TO pcl;", False),
            ("CREATE TABLE archive AS SELECT * FROM users;", False),
            (
                "WITH recent AS (SELECT * FROM logs) CREATE TABLE log_copy AS SELECT * FROM recent;",
                False,
            ),
            ("WITH x AS (DELETE FROM users RETURNING *) SELECT * FROM x;", False),
            ("UNLOAD ('SELECT * FROM users') TO 's3://bucket';", False),
            ("COPY users FROM 's3://data.csv';", False),
            (
                "MERGE INTO users USING temp_users ON users.id = temp_users.id WHEN MATCHED THEN UPDATE SET name = temp_users.name;",
                False,
            ),
            ("CALL refresh_materialized_view();", False),
            ("SET role readonly;", False),
            (
                "WITH l1 AS (WITH l2 AS (DELETE FROM users RETURNING *) SELECT * FROM l2) SELECT * FROM l1;",
                False,
            ),
            ("-- just a comment", False),
        ]

        for sql, expected_result in test_cases:
            result, msg = validate_sql(sql)
            self.assertEqual(result, expected_result, f"Failed for SQL: {sql}")
            if not result:
                self.assertIn(sql, msg, f"Unexpected message for SQL: {sql}")

    def test_multiple_statements_are_invalid(self):
        statement = [
            "SELECT * FROM users; SELECT * FROM orders;",
            "BEGIN; SELECT * FROM users; COMMIT;",
        ]
        for s in statement:
            result, msg = validate_sql(s)
            self.assertFalse(result)
            self.assertEqual(msg, "Only one SQL statement is allowed at a time.")

    def test_empty_sql(self):
        test_cases = ["", "  "]
        for sql in test_cases:
            result, msg = validate_sql(sql)
            self.assertFalse(result)
            self.assertEqual(msg, "SQL statement cannot be empty or whitespace.")


if __name__ == "__main__":
    unittest.main()
