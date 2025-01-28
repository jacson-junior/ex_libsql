defmodule ExLibSQL.Connection do
  use DBConnection

  alias ExLibSQL.Error
  alias ExLibSQL.Native.Client
  alias ExLibSQL.Native.Statement
  alias ExLibSQL.Pragma
  alias ExLibSQL.Query
  alias ExLibSQL.Result

  require Logger

  defstruct conn: nil,
            status: :idle,
            transaction_status: :idle,
            default_transaction_mode: :deferred,
            chunk_size: nil

  @doc """
  Connects to a ExLibSQL database with the specified options.

  ## Options

    * `:mode` - Required. The connection mode (:memory, :local, :local_replica, :remote, :remote_replica)
    * `:path` - Required for :local, :local_replica, and :remote_replica modes
    * `:url` - Required for :remote and :remote_replica modes
    * `:token` - Required for :remote and :remote_replica modes
    * `:flags` - Optional for :memory, :local and :local_replica [:read_only | :read_write | :create]
    * `:remote_replica_opts` - Optional for :remote_replica mode. Additional options for the remote replica connection
      * `:read_your_writes` - Optional [default: true]. Whether to read your own writes
      * `:sync_interval` - Optional. Enables syncing the replica with the primary at the specified interval in milliseconds
    * `:transaction_mode` - Optional [default: :deferred]. The transaction mode (:deferred, :immediate, :exclusive)
    * `:journal_mode` - Optional [default: :wal]. The journal mode (:wal, :delete, :truncate, :memory)
    * `:temp_store` - Optional [default: :memory]. The temp store mode (:default, :file, :memory)
    * `:synchronous` - Optional [default: :normal]. The synchronous mode (:off, :normal, :full, :extra)
    * `:foreign_keys` - Optional [default: :on]. Whether to enable foreign keys (:on, :off)
    * `:cache_size` - Optional [default: -2000]. The cache size in kilobytes
    * `:cache_spill` - Optional [default: :on]. The cache spill mode (:on, :off)
    * `:auto_vacuum` - Optional [default: :none]. The auto vacuum mode (:none, :full, :incremental)
    * `:locking_mode` - Optional [default: :normal]. The locking mode (:normal, :exclusive)
    * `:secure_delete` - Optional [default: :off]. The secure delete mode (:on, :off)
    * `:wal_auto_check_point` - Optional [default: 1000]. The WAL auto check point mode
    * `:case_sensitive_like` - Optional [default: :off]. Whether to use case sensitive LIKE (:on, :off)
    * `:busy_timeout` - Optional [default: 2000]. The busy timeout in milliseconds

  ## Examples

      iex> connect(mode: :memory)
      {:ok, connection}

      iex> connect(mode: :local, path: "path/to/db")
      {:ok, connection}

      iex> connect(mode: :remote_replica, path: "path/to/db", url: "http://example.com", token: "token")
      {:ok, connection}

  """
  @impl true
  def connect(opts) do
    with {:ok, conn} <- do_connect(opts),
         :ok <- maybe_set_pragma(conn, "journal_mode", Pragma.journal_mode(opts)),
         :ok <- set_pragma(conn, "temp_store", Pragma.temp_store(opts)),
         :ok <- set_pragma(conn, "synchronous", Pragma.synchronous(opts)),
         :ok <- set_pragma(conn, "foreign_keys", Pragma.foreign_keys(opts)),
         :ok <- maybe_set_pragma(conn, "cache_size", Pragma.cache_size(opts)),
         :ok <- set_pragma(conn, "cache_spill", Pragma.cache_spill(opts)),
         :ok <- set_pragma(conn, "auto_vacuum", Pragma.auto_vacuum(opts)),
         :ok <- set_pragma(conn, "locking_mode", Pragma.locking_mode(opts)),
         :ok <- set_pragma(conn, "secure_delete", Pragma.secure_delete(opts)),
         :ok <- set_pragma(conn, "wal_auto_check_point", Pragma.wal_auto_check_point(opts)),
         :ok <- set_pragma(conn, "case_sensitive_like", Pragma.case_sensitive_like(opts)),
         :ok <- set_pragma(conn, "busy_timeout", Pragma.busy_timeout(opts)) do
      {:ok,
       %__MODULE__{
         conn: conn,
         chunk_size: Keyword.get(opts, :chunk_size, 50)
       }}
    else
      {:error, reason} -> {:error, ExLibSQL.Error.exception(message: reason)}
    end
  end

  @impl true
  def disconnect(_err, %__MODULE__{conn: nil}) do
    :ok
  end

  def disconnect(_err, %__MODULE__{conn: conn}) do
    case conn |> Client.disconnect() do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        :ok
    end
  end

  @impl true
  def ping(%__MODULE__{conn: conn} = state) do
    case conn |> Client.query("SELECT 1") do
      {:ok, _} -> {:ok, state}
      {:error, reason} -> {:disconnect, reason, state}
    end
  end

  @impl true
  def handle_begin(opts, %{conn: conn, transaction_status: transaction_status} = state) do
    mode = Keyword.get(opts, :transaction_mode, state.default_transaction_mode)

    case transaction_status do
      :idle ->
        with {:ok, _} <- Client.begin(conn, mode),
             {:ok, transaction_status} <- Client.transaction_status(conn) do
          {:ok,
           Result.new(
             command: :begin,
             num_rows: 0,
             rows: [],
             columns: []
           ), %{state | transaction_status: transaction_status}}
        else
          {:error, reason} ->
            {:disconnect, ExLibSQL.Error.exception(message: reason), state}
        end

      :transaction ->
        savepoint_name = "savepoint_#{:erlang.unique_integer([:positive])}"

        with {:ok, _} <- Client.execute(conn, "SAVEPOINT #{savepoint_name}"),
             {:ok, transaction_status} <- Client.transaction_status(conn) do
          {:ok,
           Result.new(
             command: :begin,
             num_rows: 0,
             rows: [],
             columns: []
           ), %{state | transaction_status: transaction_status}}
        else
          {:error, reason} ->
            {:disconnect, ExLibSQL.Error.exception(message: reason), state}
        end
    end
  end

  @impl true
  def handle_commit(_opts, %{conn: conn, transaction_status: transaction_status} = state) do
    case transaction_status do
      :transaction ->
        with {:ok, _} <- Client.commit(conn),
             {:ok, transaction_status} <- Client.transaction_status(conn) do
          {:ok,
           Result.new(
             command: :commit,
             num_rows: 0,
             rows: [],
             columns: []
           ), %{state | transaction_status: transaction_status}}
        else
          {:error, reason} ->
            {:disconnect, ExLibSQL.Error.exception(message: reason), state}
        end
    end
  end

  @impl true
  def handle_rollback(_opts, %{conn: conn, transaction_status: transaction_status} = state) do
    case transaction_status do
      :transaction ->
        with {:ok, _} <- Client.rollback(conn),
             {:ok, transaction_status} <- Client.transaction_status(conn) do
          {:ok,
           Result.new(
             command: :rollback,
             num_rows: 0,
             rows: [],
             columns: []
           ), %{state | transaction_status: transaction_status}}
        else
          {:error, reason} ->
            {:disconnect, ExLibSQL.Error.exception(message: reason), state}
        end

      _ ->
        {:ok,
         Result.new(
           command: :rollback,
           num_rows: 0,
           rows: [],
           columns: []
         ), state}
    end
  end

  @impl true
  def handle_status(_opts, state) do
    {state.transaction_status, state}
  end

  @impl true
  def checkout(%__MODULE__{status: :idle} = state) do
    {:ok, %{state | status: :busy}}
  end

  def checkout(%__MODULE__{status: :busy} = state) do
    {:disconnect, ExLibSQL.Error.exception(message: "database is busy"), state}
  end

  @impl true
  def handle_prepare(
        %Query{statement: statement} = query,
        options,
        %__MODULE__{
          conn: conn
        } = state
      ) do
    query = maybe_put_command(query, options)

    with {:ok, query} <- do_prepare(conn, query, options) do
      {:ok, query, state}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  @impl true
  def handle_execute(
        %Query{} = query,
        params,
        options,
        %__MODULE__{
          conn: conn
        } = state
      ) do
    with {:ok, query} <- maybe_prepare(conn, query, options),
         {:ok, result} <- do_execute(conn, query, params),
         {:ok, _} <- Client.reset(conn, query.ref),
         {:ok, transaction_status} <- Client.transaction_status(conn) do
      {:ok, query, result, %{state | transaction_status: transaction_status}}
    else
      {:error, %Error{} = reason} ->
        {:error, %Error{reason | statement: query.statement}, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: query.statement}, state}
    end
  end

  @impl true
  def handle_declare(
        %Query{} = query,
        params,
        opts,
        %__MODULE__{
          conn: conn
        } = state
      ) do
    with {:ok, query} <- do_prepare(conn, query, opts),
         {:ok, _} <- Client.reset(conn, query.ref),
         {:ok, cursor} <- Client.cursor(conn, query.ref, params) do
      {:ok, query, cursor, state}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: query.statement}, state}
    end
  end

  @impl true
  def handle_fetch(
        %Query{statement: statement},
        cursor,
        opts,
        %__MODULE__{
          conn: conn
        } = state
      ) do
    chunk_size = opts[:chunk_size] || opts[:max_rows] || state.chunk_size

    with {:ok, result} <- Client.fetch(conn, cursor, chunk_size) do
      case result do
        {:halt, rows} ->
          {:halt, %Result{rows: rows, command: :fetch, num_rows: length(rows)}, state}

        {:continue, rows} ->
          {:cont, %Result{rows: rows, command: :fetch, num_rows: chunk_size}, state}
      end
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  @impl true
  def handle_deallocate(
        %Query{statement: statement, ref: ref},
        _cursor,
        _opts,
        %__MODULE__{
          conn: conn
        } = state
      ) do
    case Client.finalize(conn, ref) do
      {:ok, _} ->
        {:ok, nil, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  @impl true
  def handle_close(
        %Query{statement: statement, ref: ref},
        _opts,
        %__MODULE__{
          conn: conn
        } = state
      ) do
    with {:ok, _} <- Client.reset(conn, ref),
         {:ok, _} <- Client.finalize(conn, ref) do
      {:ok, nil, state}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  defp set_pragma(conn, pragma_name, value) do
    case Client.query(conn, "PRAGMA #{pragma_name} = #{value}", []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, "failed to set pragma #{pragma_name} to #{value} - #{reason}"}
    end
  end

  defp get_pragma(conn, pragma_name) do
    {:ok, statement} = Client.prepare(conn, "PRAGMA #{pragma_name}")

    case Client.query(conn, statement, []) do
      {:ok, %{rows: [[value]]}} -> {:ok, value}
      _ -> {:error, "failed to get pragma #{pragma_name}"}
    end
  end

  defp maybe_set_pragma(conn, pragma_name, value) do
    case get_pragma(conn, pragma_name) do
      {:ok, current} ->
        if current == value do
          :ok
        else
          set_pragma(conn, pragma_name, value)
        end

      _ ->
        set_pragma(conn, pragma_name, value)
    end
  end

  defp maybe_prepare(
         conn,
         %Query{
           statement: statement,
           ref: ref
         } = query,
         options
       )
       when is_nil(ref) do
    query = maybe_put_command(query, options)

    with {:ok, %Statement{} = stmt} <- Client.prepare(conn, IO.iodata_to_binary(statement)),
         query <- %{query | ref: stmt} do
      {:ok, query}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}}
    end
  end

  defp maybe_prepare(
         _conn,
         %Query{} = query,
         _options
       ) do
    {:ok, query}
  end

  defp do_connect(opts) do
    case Keyword.get(opts, :mode) do
      nil ->
        {:error, ":mode option is required"}

      :memory ->
        Client.connect({:local, ":memory:", Keyword.get(opts, :flags)})

      :local ->
        with {:ok, path} <- require_opt(opts, :path, :local) do
          Client.connect({:local, path, Keyword.get(opts, :flags)})
        end

      :local_replica ->
        with {:ok, path} <- require_opt(opts, :path, :local_replica) do
          Client.connect({:local_replica, path, Keyword.get(opts, :flags)})
        end

      :remote ->
        with {:ok, url} <- require_opt(opts, :url, :remote),
             {:ok, token} <- require_opt(opts, :token, :remote) do
          Client.connect({:remote, url, token})
        end

      :remote_replica ->
        with {:ok, path} <- require_opt(opts, :path, :remote_replica),
             {:ok, url} <- require_opt(opts, :url, :remote_replica),
             {:ok, token} <- require_opt(opts, :token, :remote_replica) do
          Client.connect(
            {:remote_replica, path, url, token, Keyword.get(opts, :remote_replica_opts)}
          )
        end

      mode ->
        {:error, "invalid mode: #{inspect(mode)}"}
    end
  end

  defp do_execute(
         conn,
         %Query{statement: statement, command: command, returns_rows?: true},
         params
       ) do
    with {:ok, result} <- Client.query(conn, IO.iodata_to_binary(statement), params) do
      {:ok,
       Result.new(
         command: command,
         num_rows: result.num_rows,
         rows: result.rows,
         columns: result.columns
       )}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}}
    end
  end

  defp do_execute(
         conn,
         %Query{statement: statement, command: command, returns_rows?: false},
         params
       ) do
    with {:ok, result} <- Client.execute(conn, IO.iodata_to_binary(statement), params) do
      {:ok,
       Result.new(
         command: command,
         num_rows: result.num_rows,
         rows: result.rows,
         columns: result.columns
       )}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}}
    end
  end

  defp do_prepare(conn, %Query{statement: statement} = query, options) do
    query = maybe_put_command(query, options)

    with {:ok, %Statement{} = stmt} <- Client.prepare(conn, IO.iodata_to_binary(statement)),
         query <- %{query | ref: stmt} do
      {:ok, query}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}}
    end
  end

  defp require_opt(opts, key, mode) do
    case Keyword.get(opts, key) do
      nil -> {:error, "`:#{key}` is required for mode `#{mode}`"}
      value -> {:ok, value}
    end
  end

  def maybe_put_command(query, options) do
    case Keyword.get(options, :command) do
      nil -> query
      command -> %{query | command: command}
    end
  end
end
