defmodule LibSQL.IntegrationTest do
  use ExUnit.Case

  alias LibSQL.Connection
  alias LibSQL.Query

  test "transaction handling with concurrent connections" do
    {:ok, conn1} =
      Connection.connect(mode: :memory)

    {:ok, conn2} =
      Connection.connect(mode: :memory)

    {:ok, _result, conn1} = Connection.handle_begin([], conn1)
    assert conn1.tx != nil
    # query = %Query{statement: "create table foo(id integer, val integer)"}
    # {:ok, _query, _result, conn1} = Connection.handle_execute(query, [], [], conn1)
    {:ok, _result, conn1} = Connection.handle_rollback([], conn1)
    assert conn1.tx == nil

    {:ok, _result, conn2} = Connection.handle_begin([], conn2)
    assert conn2.tx != nil
    # query = %Query{statement: "create table foo(id integer, val integer)"}
    # {:ok, _query, _result, conn2} = Connection.handle_execute(query, [], [], conn2)
    {:ok, _result, conn2} = Connection.handle_rollback([], conn2)
    assert conn2.tx == nil
  end
end
