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

    mississippi_config = init_arg[:mississippi_config]
    queues_config = mississippi_config[:queues]
    producer_opts = init_arg[:amqp_producer_options]

    events_producer_opts =
      Keyword.put(
        queues_config,
        :connection_options,
        producer_opts
      )

    children =
      case Keyword.get(mississippi_config, :worker_distribution_scope, :cluster) do
        :cluster ->
          distribution_strategy =
            distribution_strategy!(
              Keyword.get(mississippi_config, :cluster_distribution_strategy, :uniform)
            )

          cluster_distribution_children_spec(events_producer_opts, distribution_strategy)

        :local ->
          local_distribution_children_spec(events_producer_opts)
      end

    opts = [strategy: :rest_for_one]
    Supervisor.init(children, opts)
  end

  # fully leverage Horde supervision across the cluster
  defp cluster_distribution_children_spec(events_producer_opts, distribution_strategy) do
    [
      {Horde.Registry, [keys: :unique, name: EventsProducer.Registry, members: :auto]},
      {Horde.DynamicSupervisor,
       strategy: :one_for_one,
       name: Worker.Supervisor,
       members: :auto,
       process_redistribution: :active,
       distribution_strategy: distribution_strategy},
      {EventsProducer.NodeListener,
       [events_producer_opts: events_producer_opts, worker_distribution_scope: :cluster]}
    ]
  end

  # restrict Horde supervision to the local node
  defp local_distribution_children_spec(events_producer_opts) do
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
       [events_producer_opts: events_producer_opts, worker_distribution_scope: :local]}
    ]
  end

  defp distribution_strategy!(:uniform_quorum), do: Horde.UniformQuorumDistribution
  defp distribution_strategy!(:uniform_random), do: Horde.UniformRandomDistribution
  defp distribution_strategy!(:uniform), do: Horde.UniformDistribution
end
