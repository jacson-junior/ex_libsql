defmodule LibSQL do
  alias LibSQL.Connection

  def start_link(opts \\ []) do
    DBConnection.start_link(Connection, opts)
  end

  def child_spec(opts) do
    DBConnection.child_spec(Connection, opts)
  end
end
