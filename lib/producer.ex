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

  require Logger

  @type init_options() :: [unquote(NimbleOptions.option_typespec(Options.definition()))]

  @spec start_link([init_options()]) :: Supervisor.on_start()
  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_opts) do
    opts = NimbleOptions.validate!(init_opts, Options.definition())

    channels_per_connection = opts[:amqp_producer_options][:channels]
    mississippi_config = opts[:mississippi_config]
    queues_config = mississippi_config[:queues]
    queue_count = queues_config[:total_count]

    # Invariant: we use one channel for one queue.
    connection_number = Kernel.ceil(queue_count / channels_per_connection)

    _ =
      Logger.debug(
        "Have #{queue_count} queues and #{channels_per_connection} channels per connection"
      )

    _ =
      Logger.debug(
        "Have #{connection_number} connections a total of #{connection_number * channels_per_connection} channels"
      )

    events_producer_pool = events_producer_pool_config(connection_number)

    children = [
      {ExRabbitPool.PoolSupervisor,
       rabbitmq_config: opts[:amqp_producer_options], connection_pools: [events_producer_pool]},
      {ProducersSupervisor, mississippi_config}
    ]

    opts = [strategy: :rest_for_one]
    Supervisor.init(children, opts)
  end

  defp events_producer_pool_config(connection_number) do
    [
      name: {:local, :events_producer_pool},
      worker_module: ExRabbitPool.Worker.RabbitConnection,
      size: connection_number,
      max_overflow: 0
    ]
  end
end
