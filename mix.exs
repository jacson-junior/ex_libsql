defmodule ExLibSQL.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_libsql,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "~> 0.36.0"},
      {:db_connection, "~> 2.0"},
      {:mock, "~> 0.3.0", only: :test}
    ]
  end
end
