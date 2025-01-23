defmodule LibSQLTest do
  use ExUnit.Case
  doctest LibSQL

  test "greets the world" do
    assert LibSQL.hello() == :world
  end
end
