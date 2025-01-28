defmodule ExLibSQL.Native do
  use Rustler, otp_app: :ex_libsql, crate: "libsql_native"

  def open(_mode), do: :erlang.nif_error(:nif_not_loaded)
  def close(_conn, _pid), do: :erlang.nif_error(:nif_not_loaded)

  def execute(_conn, _sql, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def query(_conn, _sql, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)

  def prepare(_conn, _sql, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_execute(_conn, _stmt, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_query(_conn, _stmt, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_reset(_conn, _stmt, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_finalize(_conn, _stmt, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_cursor(_conn, _stmt, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_fetch(_conn, _cursor, _amount, _pid), do: :erlang.nif_error(:nif_not_loaded)

  def tx_status(_conn, _pid), do: :erlang.nif_error(:nif_not_loaded)
end
