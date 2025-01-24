defmodule LibSQL.ConnectionTest do
  use ExUnit.Case
  import Mock

  describe ".connect/1" do
    setup do
      with_mock LibSQL.Native,
        open: fn _args -> {:ok, %LibSQL.Native.Connection{conn_ref: make_ref()}} end do
        :ok
      end
    end

    test "returns error when mode is missing" do
      assert {:error, ":mode option is required"} = LibSQL.Connection.connect([])
    end

    test "returns error for invalid mode" do
      assert {:error, "invalid mode: :invalid"} = LibSQL.Connection.connect(mode: :invalid)
    end

    test "connects to in-memory database" do
      assert {:ok, %LibSQL.Connection{}} = LibSQL.Connection.connect(mode: :memory)
    end

    test ":local mode - returns error when path is missing" do
      assert {:error, "`:path` is required for mode `local`"} =
               LibSQL.Connection.connect(mode: :local)
    end

    test ":local mode - connects with valid path" do
      assert {:ok, %LibSQL.Connection{}} =
               LibSQL.Connection.connect(mode: :local, path: "test.db")
    end

    test ":local mode - connects with flags" do
      with_mock LibSQL.Native,
        open: fn {_, _, flags} -> {:ok, %LibSQL.Native.Connection{conn_ref: make_ref()}} end do
        assert {:ok, %LibSQL.Connection{}} =
                 LibSQL.Connection.connect(
                   mode: :local,
                   path: "test.db",
                   flags: [:read_only, :create]
                 )
      end
    end

    test ":local_replica mode - returns error when path is missing" do
      assert {:error, "`:path` is required for mode `local_replica`"} =
               LibSQL.Connection.connect(mode: :local_replica)
    end

    test ":local_replica mode - connects with valid path" do
      assert {:ok, %LibSQL.Connection{}} =
               LibSQL.Connection.connect(mode: :local_replica, path: "test.db")
    end

    test ":remote mode - returns error when url is missing" do
      assert {:error, "`:url` is required for mode `remote`"} =
               LibSQL.Connection.connect(mode: :remote)
    end

    test ":remote mode - returns error when token is missing" do
      assert {:error, "`:token` is required for mode `remote`"} =
               LibSQL.Connection.connect(mode: :remote, url: "http://example.com")
    end

    test ":remote mode - connects with valid url and token" do
      assert {:ok, %LibSQL.Connection{}} =
               LibSQL.Connection.connect(
                 mode: :remote,
                 url: "http://example.com",
                 token: "test_token"
               )
    end

    test ":remote_replica mode - connects with all required options and default replica opts" do
      assert {:ok, %LibSQL.Connection{}} =
               LibSQL.Connection.connect(
                 mode: :remote_replica,
                 path: "test.db",
                 url: "http://example.com",
                 token: "test_token"
               )
    end

    test ":remote_replica mode - connects with custom replica opts" do
      assert {:ok, %LibSQL.Connection{}} =
               LibSQL.Connection.connect(
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
      {:ok, conn} = LibSQL.Connection.connect(mode: :memory)
      {:ok, conn: conn}
    end

    test "disconnects successfully", %{conn: conn} do
      assert :ok = LibSQL.Connection.disconnect(nil, conn)
    end
  end

  describe ".ping/1" do
    setup do
      {:ok, conn} = LibSQL.Connection.connect(mode: :memory)
      {:ok, conn: conn}
    end

    test "returns ok when connection is alive", %{conn: conn} do
      assert {:ok, %LibSQL.Connection{}} = LibSQL.Connection.ping(conn)
    end

    test "returns disconnect when connection is closed", %{conn: conn} do
      :ok = LibSQL.Connection.disconnect(nil, conn)
      assert {:disconnect, _reason, _state} = LibSQL.Connection.ping(conn)
    end
  end
end
