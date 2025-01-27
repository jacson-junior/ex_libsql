defmodule ExLibSQL.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.
  """
  @type t :: %__MODULE__{
          statement: iodata(),
          name: atom() | String.t(),
          ref: reference() | nil,
          command: :insert | :delete | :update | nil,
          returns_rows?: boolean()
        }

  defstruct statement: nil,
            name: nil,
            ref: nil,
            command: nil,
            returns_rows?: nil

  def build(options) do
    statement = Keyword.get(options, :statement)
    name = Keyword.get(options, :name)
    ref = Keyword.get(options, :ref)

    command =
      case Keyword.get(options, :command) do
        nil -> extract_command(statement)
        value -> value
      end

    %__MODULE__{
      statement: statement,
      name: name,
      ref: ref,
      command: command,
      returns_rows?: returns_rows?(statement)
    }
  end

  @spec returns_rows?(iodata() | nil) :: boolean()
  defp returns_rows?(statement) do
    case statement do
      nil ->
        false

      statement when is_binary(statement) ->
        # Remove comments first
        clean_statement = remove_comments(statement)
        clean_statement = String.trim(clean_statement)

        cond do
          # Check for standalone SELECT statements or those in WITH clauses
          String.match?(clean_statement, ~r/^\s*(?:WITH.*?)?SELECT/i) -> true
          String.match?(clean_statement, ~r/^\s*PRAGMA/i) -> true
          String.match?(clean_statement, ~r/^\s*EXPLAIN/i) -> true
          has_returning_clause?(clean_statement) -> true
          true -> false
        end
    end
  end

  defp has_returning_clause?(query) when is_binary(query) do
    # Clean and normalize the query
    clean_query =
      query
      |> remove_comments()
      |> String.trim()
      |> String.upcase()

    # Match RETURNING clause at the end of statements
    returning_pattern = ~r/\s+RETURNING\s+(?:\*|"?[A-Z0-9_]+"?(?:\s*,\s*"?[A-Z0-9_]+"?)*)\s*$/

    String.match?(clean_query, returning_pattern)
  end

  defp remove_comments(query) when is_binary(query) do
    # Remove inline comments
    query = Regex.replace(~r/--[^\n]*/, query, "")

    # Remove multi-line comments
    Regex.replace(~r{/\*.*?\*/}s, query, "")
  end

  defp extract_command(nil), do: nil

  defp extract_command(statement) do
    cond do
      String.contains?(statement, "INSERT") -> :insert
      String.contains?(statement, "DELETE") -> :delete
      String.contains?(statement, "UPDATE") -> :update
      true -> nil
    end
  end

  defimpl DBConnection.Query do
    def parse(query, _opts) do
      query
    end

    def describe(query, _opts) do
      query
    end

    def encode(_query, params, _opts) do
      params
    end

    def decode(_query, result, _opts) do
      result
    end
  end

  defimpl String.Chars do
    def to_string(%{statement: statement}) do
      IO.iodata_to_binary(statement)
    end
  end
end
