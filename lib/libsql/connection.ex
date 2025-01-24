defmodule LibSQL.Connection do
  alias LibSQL.Native.Client
  use DBConnection

  require Logger

  defstruct conn: nil, status: :idle

  @doc """
  Connects to a LibSQL database with the specified options.

  ## Options

    * `:mode` - Required. The connection mode (:memory, :local, :local_replica, :remote, :remote_replica)
    * `:path` - Required for :local, :local_replica, and :remote_replica modes
    * `:url` - Required for :remote and :remote_replica modes
    * `:token` - Required for :remote and :remote_replica modes

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
          Client.connect({:local, ":memory:"})

        :local ->
          with {:ok, path} <- require_opt(opts, :path, :local) do
            Client.connect({:local, path})
          end

        :local_replica ->
          with {:ok, path} <- require_opt(opts, :path, :local_replica) do
            Client.connect({:local_replica, path})
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
            Client.connect({:remote_replica, path, url, token})
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
  def handle_begin(_opts, _state) do
  end

  @impl true
  def handle_commit(_opts, _state) do
  end

  @impl true
  def handle_rollback(_opts, _state) do
  end

  @impl true
  def handle_status(_opts, _state) do
  end

  @impl true
  def checkout(%__MODULE__{status: :idle} = state) do
    {:ok, %{state | status: :busy}}
  end

  def checkout(%__MODULE__{status: :busy} = state) do
    {:disconnect, "database is busy", state}
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
