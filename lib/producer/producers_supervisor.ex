# Copyright 2025 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.ProducersSupervisor do
  @moduledoc false
  use Supervisor

  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.Worker

  require Logger

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    Logger.info("ProducersSupervisor init.")

    queues_config = init_arg[:queues]

    distribution_strategy =
      distribution_strategy!(Keyword.get(init_arg, :cluster_distribution_strategy, :uniform))

    children = [
      {Horde.Registry, [keys: :unique, name: EventsProducer.Registry, members: :auto]},
      {Horde.DynamicSupervisor,
       strategy: :one_for_one,
       name: Worker.Supervisor,
       members: :auto,
       process_redistribution: :active,
       distribution_strategy: distribution_strategy},
      {EventsProducer.NodeListener, queues_config}
    ]

    opts = [strategy: :rest_for_one]
    Supervisor.init(children, opts)
  end

  defp distribution_strategy!(:uniform_quorum), do: Horde.UniformQuorumDistribution
  defp distribution_strategy!(:uniform_random), do: Horde.UniformRandomDistribution
  defp distribution_strategy!(:uniform), do: Horde.UniformDistribution
end
