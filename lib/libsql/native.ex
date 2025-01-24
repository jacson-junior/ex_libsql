defmodule LibSQL.Native do
  use Rustler, otp_app: :ex_libsql, crate: "libsql_native"

  def open(_mode), do: :erlang.nif_error(:nif_not_loaded)
  def close(_conn, _pid), do: :erlang.nif_error(:nif_not_loaded)

  def execute(_conn, _sql, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def query(_conn, _sql, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)

  def prepare(_conn, _sql, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_execute(_stmt, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stmt_query(_stmt, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)

  def transaction(_conn, _behaviour, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def commit(_tx, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def rollback(_tx, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def tx_execute(_tx, _sql, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def tx_query(_tx, _sql, _params, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def tx_prepare(_tx, _sql, _pid), do: :erlang.nif_error(:nif_not_loaded)
end
