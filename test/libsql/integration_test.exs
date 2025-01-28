defmodule ExLibSQL.IntegrationTest do
  use ExUnit.Case

  alias ExLibSQL.Query
  alias ExLibSQL.Connection

  setup do
    {:ok, pool} = ExLibSQL.start_link(mode: :memory)

    # Create necessary tables
    DBConnection.execute!(
      pool,
      Query.build(
        statement: """
          CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
          )
        """
      ),
      []
    )

    DBConnection.execute!(
      pool,
      Query.build(
        statement: """
          CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT COLLATE NOCASE,
            inserted_at DATETIME,
            updated_at DATETIME
          )
        """
      ),
      []
    )

    DBConnection.execute!(
      pool,
      Query.build(
        statement: """
          CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            type TEXT,
            tags TEXT,
            approved_at DATETIME,
            account_id INTEGER,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
          )
        """
      ),
      []
    )

    DBConnection.execute!(
      pool,
      Query.build(
        statement: """
          CREATE TABLE account_users (
            id INTEGER PRIMARY KEY,
            account_id INTEGER,
            user_id INTEGER,
            FOREIGN KEY(account_id) REFERENCES accounts(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
          )
        """
      ),
      []
    )

    {:ok, pool: pool}
  end

  describe "insert operations" do
    test "insert user", %{pool: pool} do
      # Insert first user
      {:ok, _query, %{rows: [[user1_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
          ["John"]
        )

      # Insert second user
      {:ok, _query, %{rows: [[user2_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
          ["James"]
        )

      assert user1_id != user2_id

      # Query first user
      {:ok, _query, %{rows: [[name]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT name FROM users WHERE id = ?"),
          [user1_id]
        )

      assert name == "John"
    end

    test "handles nulls correctly", %{pool: pool} do
      # Insert account
      {:ok, _query, %{rows: [[account_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO accounts (name) VALUES (?) RETURNING id"),
          ["Something"]
        )

      # Insert product
      {:ok, _query, %{rows: [[product_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(
            statement: """
              INSERT INTO products (name, account_id, approved_at, tags)
              VALUES (?, ?, ?, ?)
              RETURNING id
            """
          ),
          ["Thing", account_id, nil, "[]"]
        )

      # Query product
      {:ok, _query, %{rows: [[id, name, description, approved_at, tags]]}} =
        DBConnection.execute(
          pool,
          Query.build(
            statement: """
              SELECT id, name, description, approved_at, tags
              FROM products
              WHERE id = ?
            """
          ),
          [product_id]
        )

      assert id == product_id
      assert name == "Thing"
      assert description == nil
      assert approved_at == nil
      assert tags == "[]"
    end

    test "inserts product with type", %{pool: pool} do
      # Insert account
      {:ok, _query, %{rows: [[account_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO accounts (name) VALUES (?) RETURNING id"),
          ["Something"]
        )

      # Insert product with type
      {:ok, _query, %{rows: [[product_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(
            statement: """
              INSERT INTO products (name, type, account_id, approved_at)
              VALUES (?, ?, ?, ?)
              RETURNING id
            """
          ),
          ["Thing", "inventory", account_id, nil]
        )

      # Query product
      {:ok, _query, %{rows: [[id, name, type, approved_at]]}} =
        DBConnection.execute(
          pool,
          Query.build(
            statement: """
              SELECT id, name, type, approved_at
              FROM products
              WHERE id = ?
            """
          ),
          [product_id]
        )

      assert id == product_id
      assert name == "Thing"
      assert type == "inventory"
      assert approved_at == nil
    end

    test "insert_all", %{pool: pool} do
      # Insert user
      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO users (name) VALUES (?)"),
        ["John"]
      )

      # Get timestamp
      timestamp =
        DateTime.utc_now()
        |> DateTime.to_naive()
        |> NaiveDateTime.truncate(:second)
        |> NaiveDateTime.to_iso8601()

      # Get user's name from subquery
      {:ok, _query, %{rows: [[name]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT name FROM users WHERE name = ? AND 1=1"),
          ["John"]
        )

      # Insert account using subquery result
      {:ok, _, _} =
        DBConnection.execute(
          pool,
          Query.build(
            statement: """
              INSERT INTO accounts (name, inserted_at, updated_at)
              VALUES (?, ?, ?)
            """
          ),
          [name, timestamp, timestamp]
        )

      # Verify result
      {:ok, _query, %{rows: [[result_name]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT name FROM accounts LIMIT 1"),
          []
        )

      assert result_name == "John"
    end
  end

  describe "update operations" do
    test "updates user", %{pool: pool} do
      # Insert user
      {:ok, _query, %{rows: [[user_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
          ["John"]
        )

      # Update user
      {:ok, _, _} =
        DBConnection.execute(
          pool,
          Query.build(statement: "UPDATE users SET name = ? WHERE id = ?"),
          ["Bob", user_id]
        )

      # Verify update
      {:ok, _query, %{rows: [[name]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT name FROM users WHERE id = ?"),
          [user_id]
        )

      assert name == "Bob"
    end

    test "update_all returns correct rows", %{pool: pool} do
      # Update with no matches
      {:ok, _query, %{num_rows: rows1}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "UPDATE users SET name = ?"),
          ["WOW"]
        )

      assert rows1 == 0

      # Insert test user
      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO users (name) VALUES (?)"),
        ["Lebron James"]
      )

      # Update with no matches (specific case)
      {:ok, _query, %{num_rows: rows2}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "UPDATE users SET name = ? WHERE name = ?"),
          ["G.O.A.T", "Michael Jordan"]
        )

      assert rows2 == 0

      # Update with matches
      {:ok, _query, %{num_rows: rows3}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "UPDATE users SET name = ? WHERE name = ? RETURNING name"),
          ["G.O.A.T", "Lebron James"]
        )

      assert rows3 == 1
    end

    test "update_all handles null<->nil conversion", %{pool: pool} do
      # Insert account
      {:ok, _query, %{rows: [[account_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO accounts (name) VALUES (?) RETURNING id"),
          ["hello"]
        )

      # Update to null
      {:ok, _query, %{num_rows: updated}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "UPDATE accounts SET name = NULL"),
          []
        )

      assert updated == 1

      # Verify null value
      {:ok, _query, %{rows: [[name]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT name FROM accounts WHERE id = ?"),
          [account_id]
        )

      assert name == nil
    end
  end

  describe "delete operations" do
    test "deletes user", %{pool: pool} do
      # Insert user
      {:ok, _query, %{rows: [[user_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
          ["John"]
        )

      # Delete user
      {:ok, _, _} =
        DBConnection.execute(
          pool,
          Query.build(statement: "DELETE FROM users WHERE id = ?"),
          [user_id]
        )
    end

    test "delete all products", %{pool: pool} do
      # Insert products
      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO products (name) VALUES (?)"),
        ["hello"]
      )

      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO products (name) VALUES (?)"),
        ["hello again"]
      )

      # Delete all products
      {:ok, _query, %{num_rows: deleted}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "DELETE FROM products"),
          []
        )

      assert deleted >= 2
    end
  end

  describe "transaction operations" do
    test "successful transaction", %{pool: pool} do
      DBConnection.transaction(pool, fn conn ->
        # Insert account
        {:ok, _query, %{rows: [[account_id]]}} =
          DBConnection.execute(
            conn,
            Query.build(statement: "INSERT INTO accounts (name) VALUES (?) RETURNING id"),
            ["Foo"]
          )

        # Insert user
        {:ok, _query, %{rows: [[user_id]]}} =
          DBConnection.execute(
            conn,
            Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
            ["Bob"]
          )

        # Insert account_user
        {:ok, _, _} =
          DBConnection.execute(
            conn,
            Query.build(
              statement: """
                INSERT INTO account_users (account_id, user_id)
                VALUES (?, ?)
              """
            ),
            [account_id, user_id]
          )
      end)
    end

    test "unsuccessful transaction - invalid user", %{pool: pool} do
      assert {:ok, {:error, _}} =
               DBConnection.transaction(pool, fn conn ->
                 # Insert account
                 {:ok, _, _} =
                   DBConnection.execute(
                     conn,
                     Query.build(statement: "INSERT INTO accounts (name) VALUES (?)"),
                     ["Foo"]
                   )

                 # Try to insert invalid user (null name where not null is required)
                 DBConnection.execute(
                   conn,
                   Query.build(statement: "INSERT INTO users (name) VALUES (NULL)"),
                   []
                 )
               end)
    end
  end

  describe "preloading/joins" do
    test "joins many to many relation", %{pool: pool} do
      # Insert test data
      {:ok, _query, %{rows: [[account1_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO accounts (name) VALUES (?) RETURNING id"),
          ["Main"]
        )

      {:ok, _query, %{rows: [[account2_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO accounts (name) VALUES (?) RETURNING id"),
          ["Secondary"]
        )

      {:ok, _query, %{rows: [[user1_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
          ["John"]
        )

      {:ok, _query, %{rows: [[user2_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
          ["Shelly"]
        )

      # Create associations
      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO account_users (user_id, account_id) VALUES (?, ?)"),
        [user1_id, account1_id]
      )

      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO account_users (user_id, account_id) VALUES (?, ?)"),
        [user1_id, account2_id]
      )

      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO account_users (user_id, account_id) VALUES (?, ?)"),
        [user2_id, account2_id]
      )

      # Query with join
      {:ok, _query, %{rows: rows}} =
        DBConnection.execute(
          pool,
          Query.build(
            statement: """
              SELECT a.id, a.name, u.name as user_name
              FROM accounts a
              LEFT JOIN account_users au ON au.account_id = a.id
              LEFT JOIN users u ON u.id = au.user_id
              ORDER BY a.id, u.id
            """
          ),
          []
        )

      # One row for each account-user combination
      assert length(rows) == 3
    end
  end

  describe "select operations" do
    test "can handle IN clause", %{pool: pool} do
      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO accounts (name) VALUES (?)"),
        ["hi"]
      )

      # Test IN clause with no matches
      {:ok, _query, %{rows: rows1}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT * FROM accounts WHERE name IN (?)"),
          ["404"]
        )

      assert rows1 == []

      # Test IN clause with match
      {:ok, _query, %{rows: rows2}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT * FROM accounts WHERE name IN (?)"),
          ["hi"]
        )

      assert length(rows2) == 1
    end

    test "handles case sensitive and insensitive text", %{pool: pool} do
      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO accounts (name, email) VALUES (?, ?)"),
        ["hi", "hi@hi.com"]
      )

      # Case sensitive name
      {:ok, _query, %{rows: rows1}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT * FROM accounts WHERE name = ?"),
          ["hi"]
        )

      assert length(rows1) == 1

      {:ok, _query, %{rows: rows2}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT * FROM accounts WHERE name = ?"),
          ["HI"]
        )

      assert length(rows2) == 0

      # Case insensitive email
      {:ok, _query, %{rows: rows3}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT * FROM accounts WHERE email = ?"),
          ["hi@hi.com"]
        )

      assert length(rows3) == 1

      {:ok, _query, %{rows: rows4}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "SELECT * FROM accounts WHERE email = ?"),
          ["HI@HI.COM"]
        )

      assert length(rows4) == 1
    end

    test "handles EXISTS subquery", %{pool: pool} do
      # Insert test data
      {:ok, _query, %{rows: [[account_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO accounts (name) VALUES (?) RETURNING id"),
          ["Main"]
        )

      {:ok, _query, %{rows: [[user_id]]}} =
        DBConnection.execute(
          pool,
          Query.build(statement: "INSERT INTO users (name) VALUES (?) RETURNING id"),
          ["John"]
        )

      DBConnection.execute!(
        pool,
        Query.build(statement: "INSERT INTO account_users (user_id, account_id) VALUES (?, ?)"),
        [user_id, account_id]
      )

      # Query with EXISTS
      {:ok, _query, %{rows: rows}} =
        DBConnection.execute(
          pool,
          Query.build(
            statement: """
              SELECT * FROM accounts a
              WHERE EXISTS (
                SELECT 1 FROM account_users au
                WHERE au.account_id = a.id
              )
            """
          ),
          []
        )

      assert length(rows) == 1
    end
  end
end
