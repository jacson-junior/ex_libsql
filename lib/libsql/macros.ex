defmodule ExLibSQL.Macros do
  defmacro __using__(_opts) do
    quote do
      import ExLibSQL.Macros
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
      @spec unquote(func)(%ExLibSQL.Native.Connection{}, %ExLibSQL.Native.Statement{}, [
              String.t()
            ]) ::
              {:ok, ExLibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(
            %ExLibSQL.Native.Connection{} = conn,
            %ExLibSQL.Native.Statement{} = stmt,
            params
          ) do
        unquote(func)(conn, stmt, params, unquote(default_timeout))
      end

      @doc "Executes a prepared statement with params and custom timeout"
      @spec unquote(func)(
              %ExLibSQL.Native.Connection{},
              %ExLibSQL.Native.Statement{},
              [String.t()],
              timeout()
            ) ::
              {:ok, ExLibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(
            %ExLibSQL.Native.Connection{} = conn,
            %ExLibSQL.Native.Statement{} = stmt,
            params,
            timeout
          )
          when is_integer(timeout) do
        await_response(
          ExLibSQL.Native.unquote(:"stmt_#{native_prefix}")(
            conn.conn_ref,
            stmt.stmt_ref,
            params,
            self()
          ),
          timeout
        )
      end

      @doc "Executes a SQL statement on a connection"
      @spec unquote(func)(%ExLibSQL.Native.Connection{}, String.t()) ::
              {:ok, ExLibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%ExLibSQL.Native.Connection{} = conn, statement) do
        unquote(func)(conn, statement, [], unquote(default_timeout))
      end

      @doc "Executes a SQL statement with params on a connection"
      @spec unquote(func)(%ExLibSQL.Native.Connection{}, String.t(), [String.t()]) ::
              {:ok, ExLibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%ExLibSQL.Native.Connection{} = conn, statement, params) do
        unquote(func)(conn, statement, params, unquote(default_timeout))
      end

      @doc "Executes a SQL statement with params and custom timeout on a connection"
      @spec unquote(func)(%ExLibSQL.Native.Connection{}, String.t(), [String.t()], timeout()) ::
              {:ok, ExLibSQL.Native.Result.t()} | {:error, String.t()}
      def unquote(func)(%ExLibSQL.Native.Connection{} = conn, statement, params, timeout)
          when is_integer(timeout) do
        await_response(
          ExLibSQL.Native.unquote(:"#{native_prefix}")(conn.conn_ref, statement, params, self()),
          timeout
        )
      end
    end
  end
end
