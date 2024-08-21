defmodule Mississippi.MixProject do
  use Mix.Project

  def project do
    [
      app: :mississippi,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
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

  defp elixirc_paths(:test), do: ["test/support", "lib"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:amqp, "~> 3.3"},
      {:credo, "~> 1.0", only: [:dev], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :ci], runtime: false},
      {:efx, "~> 0.1"},
      {:elixir_uuid, "~> 1.2"},
      {:ex_check, "~> 0.16", only: [:dev], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      # hex.pm package and esl/ex_rabbit_pool do not support amqp version 2.1.
      # This fork is supporting amqp ~> 2.0 and also ~> 3.0.
      {:ex_rabbit_pool, github: "leductam/ex_rabbit_pool"},
      {:hammox, "~> 0.7", only: :test},
      {:mix_audit, "~> 2.0", only: [:dev], runtime: false},
      {:libcluster, "~> 3.3"},
      {:horde, git: "https://github.com/noaccOS/horde", ref: "feat/dynamic-supervisor-respect-extra_arguments"},
      {:nimble_options, "~> 1.0"},
      {:pretty_log, "~> 0.1"},
      {:styler, "~> 1.0.0-rc.1", only: [:dev], runtime: false},
      {:typed_struct, "~> 0.3.0"}
    ]
  end
end
