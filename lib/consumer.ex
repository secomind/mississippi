# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer do
  @moduledoc """
  This module defines the supervision tree of Mississippi.Consumer.
  """

  use Supervisor

  alias Horde.DynamicSupervisor
  alias Horde.Registry
  alias Mississippi.Consumer.AMQPDataConsumer
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.MessageTracker
  alias Mississippi.Consumer.Options

  require Logger

  @type init_options() :: [unquote(NimbleOptions.option_typespec(Options.definition()))]

  def start_link(init_opts) do
    Supervisor.start_link(__MODULE__, init_opts, name: __MODULE__)
  end

  @impl true
  def init(init_opts) do
    opts = NimbleOptions.validate!(init_opts, Options.definition())

    amqp_consumer_options = opts[:amqp_consumer_options]

    mississippi_config = opts[:mississippi_config]

    message_handler = mississippi_config[:message_handler]

    queues_config = mississippi_config[:queues]

    amqp_data_consumer_config =
      Keyword.put(queues_config, :amqp_consumer_options, amqp_consumer_options)

    distribution_strategy =
      distribution_strategy!(mississippi_config[:cluster_distribution_strategy])

    Logger.info("ConsumersSupervisor init.")

    children = [
      {Registry, [keys: :unique, name: DataUpdater.Registry, members: :auto]},
      {Registry, [keys: :unique, name: MessageTracker.Registry, members: :auto]},
      {Registry, [keys: :unique, name: AMQPDataConsumer.Registry, members: :auto]},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: DataUpdater.Supervisor,
       members: :auto,
       process_redistribution: :active,
       extra_arguments: [message_handler: message_handler],
       distribution_strategy: distribution_strategy},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: MessageTracker.Supervisor,
       members: :auto,
       process_redistribution: :active,
       distribution_strategy: distribution_strategy},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: AMQPDataConsumer.Supervisor,
       members: :auto,
       process_redistribution: :active,
       distribution_strategy: distribution_strategy},
      # This will make queue listeners start after re-sharding in a multi-node cluster
      {NodeListener, queues_config},
      # This will make queue listeners start in a single-node cluster
      {AMQPDataConsumer.Starter, amqp_data_consumer_config}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp distribution_strategy!(:uniform_quorum), do: Horde.UniformQuorumDistribution
  defp distribution_strategy!(:uniform_random), do: Horde.UniformRandomDistribution
  defp distribution_strategy!(:uniform), do: Horde.UniformDistribution
end
