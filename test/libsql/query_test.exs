defmodule ExLibSQL.QueryTest do
  use ExUnit.Case,
    async: true,
    parameterize: [
      # SELECT queries - various forms
      %{sql: "SELECT * FROM users", returns_rows?: true},
      %{sql: "SELECT * FROM users WHERE id = 1", returns_rows?: true},
      %{sql: "SELECT * FROM users ORDER BY name", returns_rows?: true},
      %{sql: "SELECT * FROM users LIMIT 10", returns_rows?: true},
      %{sql: "SELECT * FROM users GROUP BY name", returns_rows?: true},
      %{sql: "SELECT * FROM users HAVING count(*) > 1", returns_rows?: true},

      # Complex SELECT queries
      %{sql: "WITH temp AS (SELECT 1) SELECT * FROM temp", returns_rows?: true},
      %{
        sql:
          "WITH RECURSIVE temp AS (SELECT 1 UNION SELECT n + 1 FROM temp WHERE n < 10) SELECT * FROM temp",
        returns_rows?: true
      },
      %{sql: "SELECT * FROM (SELECT * FROM users) AS u", returns_rows?: true},
      %{sql: "SELECT * FROM users UNION SELECT * FROM admins", returns_rows?: true},
      %{sql: "SELECT * FROM users INTERSECT SELECT * FROM admins", returns_rows?: true},

      # INSERT queries - various forms
      %{sql: "INSERT INTO users (name) VALUES ('John') RETURNING id", returns_rows?: true},
      %{sql: "INSERT INTO users (name) VALUES ('John') RETURNING id, name", returns_rows?: true},
      %{sql: "INSERT INTO users (name) VALUES ('John') RETURNING *", returns_rows?: true},
      %{sql: "INSERT INTO users (name) SELECT name FROM temp RETURNING id", returns_rows?: true},
      %{sql: "INSERT INTO users (name) VALUES ('John')", returns_rows?: false},
      %{sql: "INSERT INTO users (name) SELECT name FROM temp", returns_rows?: false},
      %{sql: "INSERT INTO logs (message) VALUES ('User RETURNING home')", returns_rows?: false},
      %{sql: "INSERT INTO \"returning_table\" (col) VALUES ('test')", returns_rows?: false},
      %{
        sql: "INSERT INTO \"settings\" (\"properties\") VALUES (?1) RETURNING \"id\"",
        returns_rows?: true
      },

      # UPDATE queries - various forms
      %{sql: "UPDATE users SET name = 'John RETURNING' WHERE id = 1", returns_rows?: false},
      %{sql: "UPDATE users SET name = 'John' RETURNING id", returns_rows?: true},
      %{sql: "UPDATE users SET name = 'John' RETURNING id, name", returns_rows?: true},
      %{sql: "UPDATE users SET name = 'John' RETURNING *", returns_rows?: true},
      %{sql: "UPDATE users SET name = (SELECT name FROM temp) RETURNING id", returns_rows?: true},

      # DELETE queries - various forms
      %{sql: "DELETE FROM users WHERE id = 1 RETURNING *", returns_rows?: true},
      %{sql: "DELETE FROM users WHERE id = 1 RETURNING id, name", returns_rows?: true},
      %{
        sql: "DELETE FROM users WHERE id IN (SELECT id FROM temp) RETURNING *",
        returns_rows?: true
      },
      %{sql: "DELETE FROM users WHERE id = 1", returns_rows?: false},

      # Special queries
      %{sql: "EXPLAIN QUERY PLAN SELECT * FROM users", returns_rows?: true},
      %{sql: "EXPLAIN ANALYZE SELECT * FROM users", returns_rows?: true},
      %{sql: "PRAGMA table_info(users)", returns_rows?: true},
      %{sql: "PRAGMA foreign_keys = ON", returns_rows?: true},

      # Comment cases - inline comments
      %{sql: "-- RETURNING\nINSERT INTO users (name) VALUES ('John')", returns_rows?: false},
      %{sql: "INSERT INTO users (name) -- RETURNING\n VALUES ('John')", returns_rows?: false},
      %{sql: "INSERT INTO users (name) VALUES ('John') -- RETURNING", returns_rows?: false},

      # Comment cases - multi-line comments
      %{sql: "/* RETURNING */ INSERT INTO users (name) VALUES ('John')", returns_rows?: false},
      %{sql: "INSERT INTO users (name) /* RETURNING */ VALUES ('John')", returns_rows?: false},
      %{sql: "INSERT INTO users (name) VALUES ('John') /* RETURNING */", returns_rows?: false},
      %{
        sql:
          "/* comment with\n multiple RETURNING\n lines */ INSERT INTO users (name) VALUES ('John')",
        returns_rows?: false
      },

      # Mixed comments and RETURNING
      %{
        sql: "INSERT INTO users (name) VALUES ('John') /* comment */ RETURNING id",
        returns_rows?: true
      },
      %{
        sql: "INSERT INTO users (name) VALUES ('John') -- comment\nRETURNING id",
        returns_rows?: true
      },
      %{
        sql: "/* comment */ INSERT INTO users (name) VALUES ('John') RETURNING id -- comment",
        returns_rows?: true
      },

      # Edge cases with strings and identifiers
      %{
        sql: "INSERT INTO users (note) VALUES ('Something RETURNING something')",
        returns_rows?: false
      },
      %{sql: "UPDATE users SET message = 'RETURNING home' WHERE id = 1", returns_rows?: false},
      %{sql: "INSERT INTO \"RETURNING\" (col) VALUES ('test')", returns_rows?: false},
      %{sql: "SELECT * FROM \"RETURNING\"", returns_rows?: true},

      # Edge cases with spacing and newlines
      %{sql: "INSERT\nINTO\nusers\n(name)\nVALUES\n('John')\nRETURNING\nid", returns_rows?: true},
      %{sql: "INSERT INTO users (name) VALUES ('John')     RETURNING    id", returns_rows?: true},
      %{sql: "INSERT INTO users (name) VALUES ('John')\t\tRETURNING\t\tid", returns_rows?: true},

      # Transaction and other DDL queries
      %{sql: "BEGIN TRANSACTION", returns_rows?: false},
      %{sql: "COMMIT", returns_rows?: false},
      %{sql: "ROLLBACK", returns_rows?: false},
      %{sql: "CREATE TABLE users (id INTEGER PRIMARY KEY)", returns_rows?: false},
      %{sql: "DROP TABLE users", returns_rows?: false},
      %{sql: "ALTER TABLE users ADD COLUMN email TEXT", returns_rows?: false},

      # NULL and empty cases
      %{sql: nil, returns_rows?: false},
      %{sql: "", returns_rows?: false},
      %{sql: "    ", returns_rows?: false}
    ]

  test "correctly identifies queries that should use query/execute", %{
    sql: sql,
    returns_rows?: returns_rows?
  } do
    assert %{returns_rows?: ^returns_rows?} = ExLibSQL.Query.build(statement: sql)
  end
end
