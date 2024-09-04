# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer do
  @moduledoc """
  This module defines the supervision tree of Mississippi.Consumer.
  """

  use Supervisor

  alias Mississippi.Consumer.ConsumersSupervisor
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

    queue_config = opts[:mississippi_config][:queues]

    message_handler = opts[:mississippi_config][:message_handler]

    channels_per_connection = amqp_consumer_options[:channels]

    queue_count = queue_config[:range_end] - queue_config[:range_start] + 1

    # Invariant: we use one channel for one queue.
    connection_number = Kernel.ceil(queue_count / channels_per_connection)

    _ =
      Logger.debug("Have #{queue_count} queues and #{channels_per_connection} channels per connection")

    _ =
      Logger.debug(
        "Have #{connection_number} connections and a total of #{connection_number * channels_per_connection} channels"
      )

    children = [
      {ExRabbitPool.PoolSupervisor,
       rabbitmq_config: amqp_consumer_options, connection_pools: [events_consumer_pool_config(connection_number)]},
      {ConsumersSupervisor, queues: queue_config, message_handler: message_handler}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp events_consumer_pool_config(connection_number) do
    [
      name: {:local, :events_consumer_pool},
      worker_module: ExRabbitPool.Worker.RabbitConnection,
      size: connection_number,
      max_overflow: 0
    ]
  end
end
