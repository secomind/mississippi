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

    children =
      case Keyword.get(init_arg, :worker_distribution_scope, :cluster) do
        :cluster -> cluster_distribution_children_spec(init_arg)
        :local -> local_distribution_children_spec(init_arg)
      end

    opts = [strategy: :rest_for_one]
    Supervisor.init(children, opts)
  end

  # fully leverage Horde supervision across the cluster
  defp cluster_distribution_children_spec(init_arg) do
    queues_config = init_arg[:queues]

    distribution_strategy =
      distribution_strategy!(Keyword.get(init_arg, :cluster_distribution_strategy, :uniform))

    [
      {Horde.Registry, [keys: :unique, name: EventsProducer.Registry, members: :auto]},
      {Horde.DynamicSupervisor,
       strategy: :one_for_one,
       name: Worker.Supervisor,
       members: :auto,
       process_redistribution: :active,
       distribution_strategy: distribution_strategy},
      {EventsProducer.NodeListener,
       [queues_config: queues_config, worker_distribution_scope: :cluster]}
    ]
  end

  # restrict Horde supervision to the local node
  defp local_distribution_children_spec(init_arg) do
    queues_config = init_arg[:queues]

    [
      {Horde.Registry,
       [
         keys: :unique,
         name: EventsProducer.Registry,
         members: [{EventsProducer.Registry, Node.self()}]
       ]},
      {Horde.DynamicSupervisor,
       strategy: :one_for_one,
       name: Worker.Supervisor,
       members: [{Worker.Supervisor, Node.self()}]},
      {EventsProducer.NodeListener,
       [queues_config: queues_config, worker_distribution_scope: :local]}
    ]
  end

  defp distribution_strategy!(:uniform_quorum), do: Horde.UniformQuorumDistribution
  defp distribution_strategy!(:uniform_random), do: Horde.UniformRandomDistribution
  defp distribution_strategy!(:uniform), do: Horde.UniformDistribution
end
