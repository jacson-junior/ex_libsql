defmodule ExLibSQL.ConnectionTest do
  use ExUnit.Case
  import Mock

  describe ".connect/1" do
    setup do
      with_mock ExLibSQL.Native,
        open: fn _args -> {:ok, %ExLibSQL.Native.Connection{conn_ref: make_ref()}} end do
        :ok
      end
    end

    test "returns error when mode is missing" do
      assert {:error, ":mode option is required"} = ExLibSQL.Connection.connect([])
    end

    test "returns error for invalid mode" do
      assert {:error, "invalid mode: :invalid"} = ExLibSQL.Connection.connect(mode: :invalid)
    end

    test "connects to in-memory database" do
      assert {:ok, %ExLibSQL.Connection{}} = ExLibSQL.Connection.connect(mode: :memory)
    end

    test ":local mode - returns error when path is missing" do
      assert {:error, "`:path` is required for mode `local`"} =
               ExLibSQL.Connection.connect(mode: :local)
    end

    test ":local mode - connects with valid path" do
      assert {:ok, %ExLibSQL.Connection{}} =
               ExLibSQL.Connection.connect(mode: :local, path: "test.db")
    end

    test ":local mode - connects with flags" do
      with_mock ExLibSQL.Native,
        open: fn {_, _, _flags} -> {:ok, %ExLibSQL.Native.Connection{conn_ref: make_ref()}} end do
        assert {:ok, %ExLibSQL.Connection{}} =
                 ExLibSQL.Connection.connect(
                   mode: :local,
                   path: "test.db",
                   flags: [:read_only, :create]
                 )
      end
    end

    test ":local_replica mode - returns error when path is missing" do
      assert {:error, "`:path` is required for mode `local_replica`"} =
               ExLibSQL.Connection.connect(mode: :local_replica)
    end

    test ":local_replica mode - connects with valid path" do
      assert {:ok, %ExLibSQL.Connection{}} =
               ExLibSQL.Connection.connect(mode: :local_replica, path: "test.db")
    end

    test ":remote mode - returns error when url is missing" do
      assert {:error, "`:url` is required for mode `remote`"} =
               ExLibSQL.Connection.connect(mode: :remote)
    end

    test ":remote mode - returns error when token is missing" do
      assert {:error, "`:token` is required for mode `remote`"} =
               ExLibSQL.Connection.connect(mode: :remote, url: "http://example.com")
    end

    test ":remote mode - connects with valid url and token" do
      assert {:ok, %ExLibSQL.Connection{}} =
               ExLibSQL.Connection.connect(
                 mode: :remote,
                 url: "http://example.com",
                 token: "test_token"
               )
    end

    test ":remote_replica mode - connects with all required options and default replica opts" do
      assert {:ok, %ExLibSQL.Connection{}} =
               ExLibSQL.Connection.connect(
                 mode: :remote_replica,
                 path: "test.db",
                 url: "http://example.com",
                 token: "test_token"
               )
    end

    test ":remote_replica mode - connects with custom replica opts" do
      assert {:ok, %ExLibSQL.Connection{}} =
               ExLibSQL.Connection.connect(
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

  describe ".disconnect/2" do
    setup do
      {:ok, conn} = ExLibSQL.Connection.connect(mode: :memory)
      {:ok, conn: conn}
    end

    test "disconnects successfully", %{conn: conn} do
      assert :ok = ExLibSQL.Connection.disconnect(nil, conn)
    end
  end

  describe ".ping/1" do
    setup do
      {:ok, conn} = ExLibSQL.Connection.connect(mode: :memory)
      {:ok, conn: conn}
    end

    test "returns ok when connection is alive", %{conn: conn} do
      assert {:ok, %ExLibSQL.Connection{}} = ExLibSQL.Connection.ping(conn)
    end

    test "returns disconnect when connection is closed", %{conn: conn} do
      :ok = ExLibSQL.Connection.disconnect(nil, conn)
      assert {:disconnect, _reason, _state} = ExLibSQL.Connection.ping(conn)
    end
  end
end
