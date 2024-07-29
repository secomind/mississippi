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

    _ =
      Logger.debug("Have #{queue_count} queues and #{channels_per_connection} channels per connection")

    _ =
      Logger.debug(
        "Have #{connection_number} connections a total of #{connection_number * channels_per_connection} channels"
      )

    events_producer_pool = events_producer_pool_config(connection_number)

    children = [
      {ExRabbitPool.PoolSupervisor,
       rabbitmq_config: opts[:amqp_producer_options], connection_pools: [events_producer_pool]},
      {EventsProducer, opts[:mississippi_config][:queues]}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one]
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
