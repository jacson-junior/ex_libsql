defmodule LibSQL.Native.Client do
  use LibSQL.Macros
  alias LibSQL.Native.{Connection, Transaction}

  @default_timeout 5000

  @type local_mode() :: {:local, String.t()}
  @type local_replica_mode() :: {:local_replica, String.t()}
  @type remote_mode() :: {:remote, String.t(), String.t()}
  @type remote_replica_mode() :: {:remote_replica, String.t(), String.t(), String.t()}
  @type mode() :: local_mode() | local_replica_mode() | remote_mode() | remote_replica_mode()

  @spec connect(mode()) ::
          {:ok, %Connection{conn_ref: reference()}} | {:error, binary()}
  def connect(mode),
    do: LibSQL.Native.open(mode)

  @spec disconnect(Connection.t(), timeout()) :: {:ok, {}} | {:error, binary()}
  def disconnect(%Connection{} = conn, timeout \\ @default_timeout),
    do: await_response(LibSQL.Native.close(conn.conn_ref, self()), timeout)

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

  def prepare(conn, statement) do
    prepare(conn, statement, @default_timeout)
  end

  def prepare(%Connection{} = conn, statement, timeout) do
    await_response(LibSQL.Native.prepare(conn.conn_ref, statement, self()), timeout)
  end

  def prepare(%Transaction{} = tx, statement, timeout) do
    await_response(LibSQL.Native.tx_prepare(tx.tx_ref, statement, self()), timeout)
  end
end
