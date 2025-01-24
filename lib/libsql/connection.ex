defmodule LibSQL.Connection do
  use DBConnection

  alias LibSQL.Native.Client
  alias LibSQL.Result

  require Logger

  defstruct conn: nil, tx: nil, status: :idle, default_transaction_mode: :deferred

  @doc """
  Connects to a LibSQL database with the specified options.

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

      iex> connect(mode: :remote, url: "http://example.com", token: "token")
      {:ok, connection}

  """
  @impl true
  def connect(opts) do
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
    case conn |> Client.query("SELECT 1") do
      {:ok, _} -> {:ok, state}
      {:error, reason} -> {:disconnect, reason, state}
    end
  end

  @impl true
  def handle_begin(opts, %{tx: tx, conn: conn} = state) do
    mode = Keyword.get(opts, :transaction_mode, state.default_transaction_mode)

    if mode in [:deferred, :immediate, :exclusive, :read_only] do
      case tx do
        nil ->
          case Client.begin(conn, mode) do
            {:ok, new_tx} ->
              {:ok,
               Result.new(
                 command: :begin,
                 num_rows: 0,
                 rows: [],
                 columns: []
               ), %{state | tx: new_tx}}

            {:error, reason} ->
              {:disconnect, LibSQL.Error.exception(message: reason), state}
          end

        tx ->
          case Client.execute(tx, "SAVEPOINT ex_libsql_savepoint") do
            {:ok,
             %{
               num_rows: num_rows,
               rows: rows,
               columns: columns
             }} ->
              {:ok,
               Result.new(
                 command: :begin,
                 num_rows: num_rows,
                 rows: rows,
                 columns: columns
               ), state}

            {:error, reason} ->
              {:disconnect, LibSQL.Error.exception(message: reason), state}
          end
      end
    else
      {:disconnect, LibSQL.Error.exception(message: "invalid transaction mode: #{inspect(mode)}"),
       state}
    end
  end

  @impl true
  def handle_commit(_opts, %{tx: tx} = state) do
    case tx do
      nil ->
        {:disconnect, LibSQL.Error.exception(message: "no transaction to commit"), state}

      tx ->
        case Client.commit(tx) do
          {:ok, _} ->
            {:ok,
             Result.new(
               command: :commit,
               num_rows: 0,
               rows: [],
               columns: []
             ), %{state | tx: nil}}

          {:error, reason} ->
            {:disconnect, LibSQL.Error.exception(message: reason), state}
        end
    end
  end

  @impl true
  def handle_rollback(_opts, %{tx: tx} = state) do
    case tx do
      nil ->
        {:disconnect, LibSQL.Error.exception(message: "no transaction to rollback"), state}

      tx ->
        case Client.rollback(tx) do
          {:ok, _} ->
            {:ok,
             Result.new(
               command: :rollback,
               num_rows: 0,
               rows: [],
               columns: []
             ), %{state | tx: nil}}

          {:error, reason} ->
            {:disconnect, LibSQL.Error.exception(message: reason), state}
        end
    end
  end

  @impl true
  def handle_status(_opts, %{conn: conn} = state) do
    case Client.transaction_status(conn) do
      {:ok, status} ->
        {status, state}

      {:error, reason} ->
        {:disconnect, LibSQL.Error.exception(message: reason), state}
    end
  end

  @impl true
  def checkout(%__MODULE__{status: :idle} = state) do
    {:ok, %{state | status: :busy}}
  end

  def checkout(%__MODULE__{status: :busy} = state) do
    {:disconnect, LibSQL.Error.exception(message: "database is busy"), state}
  end

  @impl true
  def handle_prepare(_query, _opts, _state) do
  end

  @impl true
  def handle_execute(_query, _params, _opts, _state) do
  end

  @impl true
  def handle_declare(_query, _cursor, _opts, _state) do
  end

  @impl true
  def handle_fetch(_query, _cursor, _opts, _state) do
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, _state) do
  end

  @impl true
  def handle_close(_query, _opts, _state) do
  end

  defp require_opt(opts, key, mode) do
    case Keyword.get(opts, key) do
      nil -> {:error, "`:#{key}` is required for mode `#{mode}`"}
      value -> {:ok, value}
    end
  end
end
