defmodule ExLibSQL.Connection do
  use DBConnection

  alias ExLibSQL.Native.Statement
  alias ExLibSQL.Error
  alias ExLibSQL.Query
  alias ExLibSQL.Native.Client
  alias ExLibSQL.Result

  require Logger

  defstruct conn: nil,
            tx: nil,
            status: :idle,
            transaction_status: :idle,
            default_transaction_mode: :deferred,
            chunk_size: 50

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
    * `:transaction_mode` - Optional [default: :deferred]. The transaction mode (:deferred, :immediate, :exclusive, :read_only)

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
    Logger.debug("Connecting to database with options: #{inspect(opts)}")

    result =
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

    case result do
      {:ok, conn} -> {:ok, %__MODULE__{conn: conn}}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def disconnect(_err, %__MODULE__{conn: conn}) do
    Logger.debug("Disconnecting from database")

    case conn |> Client.disconnect() do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to disconnect: #{reason}")
        :ok
    end
  end

  @impl true
  def ping(%__MODULE__{conn: conn} = state) do
    Logger.debug("Pinging database")

    case conn |> Client.query("SELECT 1") do
      {:ok, _} -> {:ok, state}
      {:error, reason} -> {:disconnect, reason, state}
    end
  end

  @impl true
  def handle_begin(opts, %{tx: tx, conn: conn, transaction_status: transaction_status} = state) do
    Logger.debug("Beginning transaction with options: #{inspect(opts)}")

    mode = Keyword.get(opts, :transaction_mode, state.default_transaction_mode)

    if mode in [:deferred, :immediate, :exclusive, :read_only] do
      case transaction_status do
        :idle ->
          with {:ok, new_tx} <- Client.begin(conn, mode),
               {:ok, transaction_status} <- Client.transaction_status(conn) do
            {:ok,
             Result.new(
               command: :begin,
               num_rows: 0,
               rows: [],
               columns: []
             ), %{state | tx: new_tx, transaction_status: transaction_status}}
          else
            {:error, reason} ->
              {:disconnect, ExLibSQL.Error.exception(message: reason), state}
          end

        :transaction ->
          with {:ok,
                %{
                  num_rows: num_rows,
                  rows: rows,
                  columns: columns
                }} <- Client.execute(tx, "SAVEPOINT ex_libsql_savepoint"),
               {:ok, transaction_status} <- Client.transaction_status(conn) do
            {:ok,
             Result.new(
               command: :begin,
               num_rows: num_rows,
               rows: rows,
               columns: columns
             ), %{state | transaction_status: transaction_status}}
          else
            {:error, reason} ->
              {:disconnect, ExLibSQL.Error.exception(message: reason), state}
          end
      end
    else
      {:disconnect,
       ExLibSQL.Error.exception(message: "invalid transaction mode: #{inspect(mode)}"), state}
    end
  end

  @impl true
  def handle_commit(_opts, %{tx: tx, conn: conn, transaction_status: transaction_status} = state) do
    Logger.debug("Committing transaction")

    case transaction_status do
      :transaction ->
        with {:ok, _} <- Client.commit(tx),
             {:ok, transaction_status} <- Client.transaction_status(conn) do
          {:ok,
           Result.new(
             command: :commit,
             num_rows: 0,
             rows: [],
             columns: []
           ), %{state | tx: nil, transaction_status: transaction_status}}
        else
          {:error, reason} ->
            {:disconnect, ExLibSQL.Error.exception(message: reason), state}
        end
    end
  end

  @impl true
  def handle_rollback(
        _opts,
        %{tx: tx, conn: conn, transaction_status: transaction_status} = state
      ) do
    Logger.debug("Rolling back transaction")

    case transaction_status do
      :transaction ->
        with {:ok, _} <- Client.rollback(tx),
             {:ok, transaction_status} <- Client.transaction_status(conn) do
          {:ok,
           Result.new(
             command: :rollback,
             num_rows: 0,
             rows: [],
             columns: []
           ), %{state | tx: nil, transaction_status: transaction_status}}
        else
          {:error, reason} ->
            {:disconnect, ExLibSQL.Error.exception(message: reason), state}
        end
    end
  end

  @impl true
  def handle_status(_opts, state) do
    {state.transaction_status, state}
  end

  @impl true
  def checkout(%__MODULE__{status: :idle} = state) do
    Logger.debug("Checking out database connection")
    {:ok, %{state | status: :busy}}
  end

  def checkout(%__MODULE__{status: :busy} = state) do
    Logger.debug("Database is busy")
    {:disconnect, ExLibSQL.Error.exception(message: "database is busy"), state}
  end

  @impl true
  def handle_prepare(
        %Query{statement: statement} = query,
        options,
        %__MODULE__{
          conn: conn,
          tx: tx
        } = state
      ) do
    Logger.debug("Preparing query: #{inspect(query)}")
    query = maybe_put_command(query, options)

    with {:ok, query} <- do_prepare(tx || conn, query, options) do
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
          conn: conn,
          tx: tx
        } = state
      ) do
    Logger.debug("Executing query: #{inspect(query)} with params: #{inspect(params)}")

    with {:ok, query} <- maybe_prepare(tx || conn, query, options),
         {:ok, result} <- do_execute(tx || conn, query, params),
         {:ok, transaction_status} <- Client.transaction_status(conn) do
      {:ok, query, result, %{state | transaction_status: transaction_status}}
    else
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
          conn: conn,
          tx: tx
        } = state
      ) do
    Logger.debug("Declaring cursor: #{inspect(query)}")

    with {:ok, query} <- do_prepare(tx || conn, query, opts),
         {:ok, cursor} <- Client.cursor(query.ref, params) do
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
        %__MODULE__{} = state
      ) do
    Logger.debug("Fetching cursor: #{inspect(cursor)}")
    chunk_size = opts[:chunk_size] || opts[:max_rows] || state.chunk_size

    with {:ok, result} <- Client.fetch(cursor, chunk_size) do
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
        state
      ) do
    Logger.debug("Deallocating cursor: #{inspect(ref)}")

    case Client.finalize(ref) do
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
        state
      ) do
    Logger.debug("Closing cursor: #{inspect(ref)}")

    case Client.finalize(ref) do
      {:ok, _} ->
        {:ok, nil, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
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
