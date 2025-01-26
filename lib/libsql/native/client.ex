defmodule LibSQL.Native.Client do
  use LibSQL.Macros
  alias LibSQL.Native.{Connection, Cursor, Statement, Transaction}

  import Bitwise
  import Record
  defrecord :remote_opts, [:read_your_writes, :sync_interval]

  @default_timeout 5000
  # SQLITE_OPEN_READONLY
  @open_read_only 0b0001
  # SQLITE_OPEN_READWRITE
  @open_read_write 0b0010
  # SQLITE_OPEN_CREATE
  @open_create 0b0100

  @type remote_opts() :: keyword() | nil
  @type local_mode() :: {:local, String.t()} | {:local, String.t(), keyword()}
  @type local_replica_mode() ::
          {:local_replica, String.t()} | {:local_replica, String.t(), keyword()}
  @type remote_mode() :: {:remote, String.t(), String.t()}
  @type remote_replica_mode() ::
          {:remote_replica, String.t(), String.t(), String.t(), remote_opts()}
  @type mode() :: local_mode() | local_replica_mode() | remote_mode() | remote_replica_mode()
  @type flag() :: :read_only | :read_write | :create

  @spec connect(mode()) ::
          {:ok, %Connection{conn_ref: reference()}} | {:error, binary()}
  def connect({mode, path, flags})
      when mode in [:local, :local_replica] and is_nil(flags) == false do
    flag_int =
      Enum.reduce(flags, 0, fn
        :read_only, acc -> acc ||| @open_read_only
        :read_write, acc -> acc ||| @open_read_write
        :create, acc -> acc ||| @open_create
      end)

    LibSQL.Native.open({mode, path, flag_int})
  end

  def connect({mode, path, url, token, opts})
      when mode == :remote_replica and is_nil(opts) == false do
    LibSQL.Native.open(
      {mode, path, url, token,
       remote_opts(
         read_your_writes: Keyword.get(opts, :read_your_writes),
         sync_interval: Keyword.get(opts, :sync_interval)
       )}
    )
  end

  def connect(mode), do: LibSQL.Native.open(mode)

  @spec disconnect(Connection.t(), timeout()) :: {:ok, {}} | {:error, binary()}
  def disconnect(%Connection{} = conn, timeout \\ @default_timeout),
    do: await_response(LibSQL.Native.close(conn.conn_ref, self()), timeout)

  define_operations(:execute, @default_timeout)
  define_operations(:query, @default_timeout)

  def begin(%Connection{} = conn, behaviour \\ :deferred, timeout \\ @default_timeout) do
    await_response(LibSQL.Native.begin(conn.conn_ref, behaviour, self()), timeout)
  end

  def commit(%Transaction{} = tx, timeout \\ @default_timeout) do
    await_response(LibSQL.Native.commit(tx.tx_ref, self()), timeout)
  end

  def rollback(%Transaction{} = tx, timeout \\ @default_timeout) do
    await_response(LibSQL.Native.rollback(tx.tx_ref, self()), timeout)
  end

  def transaction_status(%Connection{} = conn, timeout \\ @default_timeout) do
    await_response(LibSQL.Native.tx_status(conn.conn_ref, self()), timeout)
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

  def cursor(%Statement{} = stmt, params) do
    await_response(LibSQL.Native.stmt_cursor(stmt.stmt_ref, params, self()), @default_timeout)
  end

  def cursor(%Statement{} = stmt, params, timeout) do
    await_response(LibSQL.Native.stmt_cursor(stmt.stmt_ref, params, self()), timeout)
  end

  def fetch(%Cursor{} = cursor, amount) do
    await_response(LibSQL.Native.stmt_fetch(cursor.cur_ref, amount, self()), @default_timeout)
  end

  def fetch(%Cursor{} = cursor, amount, timeout) do
    await_response(LibSQL.Native.stmt_fetch(cursor.cur_ref, amount, self()), timeout)
  end

  def finalize(%Statement{} = stmt) do
    await_response(LibSQL.Native.stmt_finalize(stmt.stmt_ref, self()), @default_timeout)
  end

  def finalize(%Statement{} = stmt, timeout) do
    await_response(LibSQL.Native.stmt_finalize(stmt.stmt_ref, self()), timeout)
  end
end
