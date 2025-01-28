defmodule ConnectionTest do
  use ExUnit.Case

  alias ExLibSQL.Query
  alias ExLibSQL.Result
  alias ExLibSQL.Native.Client
  alias ExLibSQL.Native.Statement
  alias ExLibSQL.Error
  alias ExLibSQL.Connection

  import Mock

  describe ".connect/1 options" do
    setup_with_mocks([
      {Client, [],
       [
         connect: fn _mode -> {:ok, %Connection{}} end,
         prepare: fn _conn, _stmt -> {:ok, %Statement{}} end,
         query: fn _conn, _stmt -> {:ok, %Result{}} end,
         query: fn _conn, _stmt, _params -> {:ok, %Result{}} end,
         disconnect: fn _conn -> {:ok, {}} end
       ]}
    ]) do
      :ok
    end

    test "returns error when mode is missing" do
      assert {:error, %Error{message: ":mode option is required"}} =
               Connection.connect([])
    end

    test "returns error for invalid mode" do
      assert {:error, %Error{message: "invalid mode: :invalid"}} =
               Connection.connect(mode: :invalid)
    end

    test "connects to in-memory database" do
      assert {:ok, %Connection{}} = Connection.connect(mode: :memory)
    end

    test ":local mode - returns error when path is missing" do
      assert {:error, %Error{message: "`:path` is required for mode `local`"}} =
               Connection.connect(mode: :local)
    end

    test ":local mode - connects with valid path" do
      assert {:ok, %Connection{}} =
               Connection.connect(mode: :local, path: "test.db")
    end

    test ":local mode - connects with flags" do
      assert {:ok, %Connection{}} =
               Connection.connect(
                 mode: :local,
                 path: "test.db",
                 flags: [:read_only, :create]
               )
    end

    test ":local_replica mode - returns error when path is missing" do
      assert {:error, %Error{message: "`:path` is required for mode `local_replica`"}} =
               Connection.connect(mode: :local_replica)
    end

    test ":local_replica mode - connects with valid path" do
      assert {:ok, %Connection{}} =
               Connection.connect(mode: :local_replica, path: "test.db")
    end

    test ":remote mode - returns error when url is missing" do
      assert {:error, %Error{message: "`:url` is required for mode `remote`"}} =
               Connection.connect(mode: :remote)
    end

    test ":remote mode - returns error when token is missing" do
      assert {:error, %Error{message: "`:token` is required for mode `remote`"}} =
               Connection.connect(mode: :remote, url: "http://example.com")
    end

    test ":remote mode - connects with valid url and token" do
      assert {:ok, %Connection{}} =
               Connection.connect(
                 mode: :remote,
                 url: "http://example.com",
                 token: "test_token"
               )
    end

    test ":remote_replica mode - connects with all required options and default replica opts" do
      assert {:ok, %Connection{}} =
               Connection.connect(
                 mode: :remote_replica,
                 path: "test.db",
                 url: "http://example.com",
                 token: "test_token"
               )
    end

    test ":remote_replica mode - connects with custom replica opts" do
      assert {:ok, %Connection{}} =
               Connection.connect(
                 mode: :remote_replica,
                 path: "test.db",
                 url: "http://example.com",
                 token: "test_token",
                 remote_replica_opts: [
                   read_your_writes: false,
                   sync_interval: 1000
                 ]
               )
    end
  end

  describe ".connect/2" do
    test "fails to write with read-only flag" do
      path = Temp.path!()

      {:ok, conn} = Connection.connect(mode: :local, path: path)

      {:ok, _, _, _} =
        Connection.handle_execute(
          Query.build(statement: "create table test (id integer primary key, stuff text)"),
          [],
          [],
          conn
        )

      {:ok, _, _, _} =
        Connection.handle_execute(
          Query.build(statement: "insert into test (id, stuff) values (999, 'Some stuff')"),
          [],
          [],
          conn
        )

      :ok = Connection.disconnect(nil, conn)

      {:ok, ro_conn} = Connection.connect(mode: :memory, flags: [:read_only])

      {:ok, _, result, _} =
        Connection.handle_execute(
          Query.build(statement: "select count(*) from test"),
          [],
          [],
          ro_conn
        )

      assert result.rows == [[1]]

      {:error, error, _} =
        Connection.handle_execute(
          Query.build(statement: "insert into test (id, stuff) values (888, 'more stuff')"),
          [],
          [],
          ro_conn
        )

      assert error.message =~ "attempt to write a readonly database"
    end
  end

  describe ".disconnect/2" do
    test "disconnects a database that was never connected" do
      conn = %Connection{conn: nil}
      assert :ok == Connection.disconnect(nil, conn)
    end

    test "disconnects a connected database" do
      {:ok, conn} = Connection.connect(mode: :memory)
      assert :ok == Connection.disconnect(nil, conn)
    end
  end

  describe ".handle_execute/4" do
    test "returns records" do
      {:ok, conn} = Connection.connect(mode: :memory)

      # Create table and insert data
      {:ok, _, _, conn} =
        Connection.handle_execute(
          Query.build(statement: "create table users (id integer primary key, name text)"),
          [],
          [],
          conn
        )

      {:ok, _, _, conn} =
        Connection.handle_execute(
          Query.build(statement: "insert into users (id, name) values (?, ?)"),
          [1, "Jim"],
          [],
          conn
        )

      # Query the data
      {:ok, _query, result, _conn} =
        Connection.handle_execute(
          Query.build(statement: "select * from users where id = ?"),
          [1],
          [],
          conn
        )

      assert result.command == nil
      assert result.columns == ["id", "name"]
      assert result.rows == [[1, "Jim"]]
    end
  end

  describe ".ping/1" do
    test "returns ok when connection is alive" do
      {:ok, conn} = Connection.connect(mode: :memory)
      assert {:ok, %Connection{}} = Connection.ping(conn)
    end

    test "returns disconnect when connection is closed" do
      {:ok, conn} = Connection.connect(mode: :memory)
      :ok = Connection.disconnect(nil, conn)
      assert {:disconnect, _reason, _state} = Connection.ping(conn)
    end
  end
end
