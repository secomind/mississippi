# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer do
  @moduledoc """
  This module defines the supervision tree of Mississippi.Producer.
  """

  # Automatically defines child_spec/1
  use Supervisor

  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.Options

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
    queue_count = opts[:mississippi_config][:queues][:total_count]

    # Invariant: we use one channel for one queue.
    connection_number = Kernel.ceil(queue_count / channels_per_connection)

    events_producer_opts =
      Keyword.put(opts[:mississippi_config][:queues], :connection_options, opts[:amqp_producer_options])

    Logger.debug("Have #{queue_count} queues and #{channels_per_connection} channels per connection")

    Logger.debug(
      "Have #{connection_number} connections a total of #{connection_number * channels_per_connection} channels"
    )

    children = [
      {EventsProducer, events_producer_opts}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one]
    Supervisor.init(children, opts)
  end
end
