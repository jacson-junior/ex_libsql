defmodule LibSQL.IntegrationTest do
  use ExUnit.Case

  alias LibSQL.Connection
  alias LibSQL.Query

  test "transaction handling with concurrent connections" do
    {:ok, conn1} =
      Connection.connect(mode: :memory)

    {:ok, conn2} =
      Connection.connect(mode: :memory)

    {status, conn1} = Connection.handle_status([], conn1)
    assert status == :idle
    {:ok, _result, conn1} = Connection.handle_begin([], conn1)
    {status, conn1} = Connection.handle_status([], conn1)
    assert status == :transaction
    {:ok, _result, conn1} = Connection.handle_rollback([], conn1)
    {status, _conn1} = Connection.handle_status([], conn1)
    assert status == :idle

    {status, conn2} = Connection.handle_status([], conn2)
    assert status == :idle
    {:ok, _result, conn2} = Connection.handle_begin([], conn2)
    {status, conn2} = Connection.handle_status([], conn2)
    assert status == :transaction
    {:ok, _result, conn2} = Connection.handle_rollback([], conn2)
    {status, _conn2} = Connection.handle_status([], conn2)
    assert status == :idle
  end
end
