defmodule LibSQL do
  use LibSQL.Macros
  alias LibSQL.Native.{Connection, Transaction}

  @default_timeout 5000

  def connect(mode), do: LibSQL.Native.open(mode)
  def disconnect(%Connection{} = conn), do: LibSQL.Native.close(conn.conn_ref)

  define_operations(:execute, @default_timeout)
  define_operations(:query, @default_timeout)

  def transaction(%Connection{} = conn, behaviour \\ :deferred, timeout \\ @default_timeout) do
    await_response(LibSQL.Native.transaction(conn.conn_ref, behaviour, self()), timeout)
  end

  def commit(%Transaction{} = tx, timeout \\ @default_timeout) do
    await_response(LibSQL.Native.commit(tx.tx_ref, self()), timeout)
  end

  def rollback(%Transaction{} = tx, timeout \\ @default_timeout) do
    await_response(LibSQL.Native.rollback(tx.tx_ref, self()), timeout)
  end

  def prepare(conn_or_tx, statement) do
    prepare(conn_or_tx, statement, @default_timeout)
  end

  def prepare(%Connection{} = conn, statement, timeout) do
    await_response(LibSQL.Native.prepare(conn.conn_ref, statement, self()), timeout)
  end

  def prepare(%Transaction{} = tx, statement, timeout) do
    await_response(LibSQL.Native.tx_prepare(tx.tx_ref, statement, self()), timeout)
  end
end
