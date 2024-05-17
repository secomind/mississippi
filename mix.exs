defmodule Mississippi.MixProject do
  use Mix.Project

  def project do
    [
      app: :mississippi,
      version: "0.1.0",
      elixir: "~> 1.15",
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
      {:amqp, "~> 3.3"},
      {:dialyxir, "~> 1.4", only: [:dev, :ci], runtime: false},
      {:elixir_uuid, "~> 1.2"},
      {:excoveralls, "~> 0.18", only: :test},
      {:nimble_options, "~> 1.0"},
      {:pretty_log, "~> 0.1"},
      # hex.pm package and esl/ex_rabbit_pool do not support amqp version 2.1.
      # This fork is supporting amqp ~> 2.0 and also ~> 3.0.
      {:ex_rabbit_pool, github: "leductam/ex_rabbit_pool"},
      {:skogsra, "~> 2.5"}
    ]
  end
end
