defmodule LibSQL.Macros do
  defmacro __using__(_opts) do
    quote do
      import LibSQL.Macros
    end
  end

  @spec await_response(term(), timeout()) :: Macro.t()
  defmacro await_response(native_call, timeout) do
    quote do
      case unquote(native_call) do
        {:ok, _} ->
          receive do
            data -> data
          after
            unquote(timeout) -> {:error, :timeout}
          end

        err ->
          err
      end
    end
  end

  defmacro define_operations(func, default_timeout) do
    native_prefix = to_string(func)

    quote location: :keep do
      @doc "Executes a prepared statement with params"
      @spec unquote(func)(%LibSQL.Native.Statement{}, [String.t()]) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Statement{} = stmt, params) do
        unquote(func)(stmt, params, unquote(default_timeout))
      end

      @doc "Executes a prepared statement with params and custom timeout"
      @spec unquote(func)(%LibSQL.Native.Statement{}, [String.t()], timeout()) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Statement{} = stmt, params, timeout)
          when is_integer(timeout) do
        await_response(
          LibSQL.Native.unquote(:"stmt_#{native_prefix}")(stmt.stmt_ref, params, self()),
          timeout
        )
      end

      @doc "Executes a SQL statement on a connection"
      @spec unquote(func)(%LibSQL.Native.Connection{}, String.t()) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Connection{} = conn, statement) do
        unquote(func)(conn, statement, [], unquote(default_timeout))
      end

      @doc "Executes a SQL statement with params on a connection"
      @spec unquote(func)(%LibSQL.Native.Connection{}, String.t(), [String.t()]) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Connection{} = conn, statement, params) do
        unquote(func)(conn, statement, params, unquote(default_timeout))
      end

      @doc "Executes a SQL statement with params and custom timeout on a connection"
      @spec unquote(func)(%LibSQL.Native.Connection{}, String.t(), [String.t()], timeout()) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Connection{} = conn, statement, params, timeout)
          when is_integer(timeout) do
        await_response(
          LibSQL.Native.unquote(:"#{native_prefix}")(conn.conn_ref, statement, params, self()),
          timeout
        )
      end

      @doc "Executes a SQL statement in a transaction"
      @spec unquote(func)(%LibSQL.Native.Transaction{}, String.t()) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Transaction{} = tx, statement) do
        unquote(func)(tx, statement, [], unquote(default_timeout))
      end

      @doc "Executes a SQL statement with params in a transaction"
      @spec unquote(func)(%LibSQL.Native.Transaction{}, String.t(), [String.t()]) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Transaction{} = tx, statement, params) do
        unquote(func)(tx, statement, params, unquote(default_timeout))
      end

      @doc "Executes a SQL statement with params and custom timeout in a transaction"
      @spec unquote(func)(%LibSQL.Native.Transaction{}, String.t(), [String.t()], timeout()) ::
              {:ok, LibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%LibSQL.Native.Transaction{} = tx, statement, params, timeout)
          when is_integer(timeout) do
        await_response(
          LibSQL.Native.unquote(:"tx_#{native_prefix}")(tx.tx_ref, statement, params, self()),
          timeout
        )
      end
    end
  end
end
