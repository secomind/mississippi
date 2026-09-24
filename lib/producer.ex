# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer do
  @moduledoc """
  This module defines the supervision tree of Mississippi.Producer.
  """

  # Automatically defines child_spec/1
  use Supervisor

  alias Mississippi.Producer.Options
  alias Mississippi.Producer.ProducersSupervisor

  @type init_options() :: [unquote(NimbleOptions.option_typespec(Options.definition()))]

  @spec start_link([init_options()]) :: Supervisor.on_start()
  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_opts) do
    opts = NimbleOptions.validate!(init_opts, Options.definition())

    children = [
      {ProducersSupervisor, opts}
    ]

    opts = [strategy: :rest_for_one]
    Supervisor.init(children, opts)
  end
end
