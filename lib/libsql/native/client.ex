defmodule ExLibSQL.Native.Client do
  use ExLibSQL.Macros
  alias ExLibSQL.Native.{Connection, Cursor, Statement}

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

  @spec connect(mode()) :: {:ok, %Connection{}} | {:error, binary()}
  def connect({mode, path, flags})
      when mode in [:local, :local_replica] and is_nil(flags) == false do
    flag_int =
      Enum.reduce(flags, 0, fn
        :read_only, acc -> acc ||| @open_read_only
        :read_write, acc -> acc ||| @open_read_write
        :create, acc -> acc ||| @open_create
      end)

    ExLibSQL.Native.open({mode, path, flag_int})
  end

  def connect({mode, path, url, token, opts})
      when mode == :remote_replica and is_nil(opts) == false do
    ExLibSQL.Native.open(
      {mode, path, url, token,
       remote_opts(
         read_your_writes: Keyword.get(opts, :read_your_writes),
         sync_interval: Keyword.get(opts, :sync_interval)
       )}
    )
  end

  def connect(mode), do: ExLibSQL.Native.open(mode)

  @spec disconnect(Connection.t(), timeout()) :: {:ok, {}} | {:error, binary()}
  def disconnect(%Connection{} = conn, timeout \\ @default_timeout),
    do: await_response(ExLibSQL.Native.close(conn.conn_ref, self()), timeout)

  define_operations(:execute, @default_timeout)
  define_operations(:query, @default_timeout)

  def begin(%Connection{} = conn, behaviour \\ :deferred, timeout \\ @default_timeout) do
    sql =
      case behaviour do
        :deferred -> "BEGIN DEFERRED TRANSACTION"
        :immediate -> "BEGIN IMMEDIATE TRANSACTION"
        :exclusive -> "BEGIN EXCLUSIVE TRANSACTION"
        :read_only -> "BEGIN TRANSACTION READ ONLY"
      end

    await_response(
      ExLibSQL.Native.execute(conn.conn_ref, sql, [], self()),
      timeout
    )
  end

  def commit(%Connection{} = conn, timeout \\ @default_timeout) do
    await_response(
      ExLibSQL.Native.execute(conn.conn_ref, "COMMIT TRANSACTION", [], self()),
      timeout
    )
  end

  def rollback(%Connection{} = conn, timeout \\ @default_timeout) do
    await_response(
      ExLibSQL.Native.execute(conn.conn_ref, "ROLLBACK", [], self()),
      timeout
    )
  end

  def transaction_status(%Connection{} = conn, timeout \\ @default_timeout) do
    await_response(ExLibSQL.Native.tx_status(conn.conn_ref, self()), timeout)
  end

  def prepare(conn, statement) do
    prepare(conn, statement, @default_timeout)
  end

  def prepare(%Connection{} = conn, statement, timeout) do
    await_response(ExLibSQL.Native.prepare(conn.conn_ref, statement, self()), timeout)
  end

  def cursor(%Connection{} = conn, %Statement{} = stmt, params) do
    await_response(
      ExLibSQL.Native.stmt_cursor(conn.conn_ref, stmt.stmt_ref, params, self()),
      @default_timeout
    )
  end

  def cursor(%Connection{} = conn, %Statement{} = stmt, params, timeout) do
    await_response(
      ExLibSQL.Native.stmt_cursor(conn.conn_ref, stmt.stmt_ref, params, self()),
      timeout
    )
  end

  def fetch(%Connection{} = conn, %Cursor{} = cursor, amount) do
    await_response(
      ExLibSQL.Native.stmt_fetch(conn.conn_ref, cursor.cur_ref, amount, self()),
      @default_timeout
    )
  end

  def fetch(%Connection{} = conn, %Cursor{} = cursor, amount, timeout) do
    await_response(
      ExLibSQL.Native.stmt_fetch(conn.conn_ref, cursor.cur_ref, amount, self()),
      timeout
    )
  end

  def reset(%Connection{} = conn, %Statement{} = stmt) do
    await_response(
      ExLibSQL.Native.stmt_reset(conn.conn_ref, stmt.stmt_ref, self()),
      @default_timeout
    )
  end

  def reset(%Connection{} = conn, %Statement{} = stmt, timeout) do
    await_response(
      ExLibSQL.Native.stmt_reset(conn.conn_ref, stmt.stmt_ref, self()),
      timeout
    )
  end

  def finalize(%Connection{} = conn, %Statement{} = stmt) do
    await_response(
      ExLibSQL.Native.stmt_finalize(conn.conn_ref, stmt.stmt_ref, self()),
      @default_timeout
    )
  end

  def finalize(%Connection{} = conn, %Statement{} = stmt, timeout) do
    await_response(ExLibSQL.Native.stmt_finalize(conn.conn_ref, stmt.stmt_ref, self()), timeout)
  end
end
