# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.MixProject do
  use Mix.Project

  @source_url "https://github.com/secomind/mississippi"
  @version "1.0.0"

  def project do
    [
      app: :mississippi,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Mississippi",
      description: description(),
      package: package(),
      source_url: @source_url
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:amqp, "~> 3.3"},
      {:credo, "~> 1.0", only: [:dev], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :ci], runtime: false},
      {:efx, "~> 0.1"},
      {:elixir_uuid, "~> 1.2"},
      {:ex_check, "~> 0.16", only: [:dev], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:current_rabbit_pool, "~> 1.1"},
      {:hammox, "~> 0.7", only: :test},
      {:mix_audit, "~> 2.0", only: [:dev], runtime: false},
      {:nimble_options, "~> 1.0"},
      {:pretty_log, "~> 0.1"},
      {:styler, "~> 1.0.0-rc.1", only: [:dev], runtime: false},
      {:typed_struct, "~> 0.3.0"}
    ]
  end

  defp description do
    """
    A framework for handling distributed and ordered processing of data over AMQP queues.
    """
  end

  defp elixirc_paths(:test), do: ["test/support", "lib"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      organization: "",
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      maintainers: ["Arnaldo Cesco", "Francesco Noacco"]
    ]
  end
end
