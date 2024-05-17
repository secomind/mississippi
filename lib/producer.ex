defmodule Mississippi.Producer do
  @moduledoc """
  This module defines the supervision tree of Mississippi.Producer.
  """

  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.Options

  # Automatically defines child_spec/1
  use Supervisor

  @type ssl_option ::
          {:cacertfile, String.t()}
          | {:verify, :verify_peer}
          | {:server_name_indication, charlist() | :disable}
          | {:depth, integer()}
  @type ssl_options :: :none | [ssl_option]

  @type amqp_options ::
          {:username, String.t()}
          | {:password, String.t()}
          | {:virtual_host, String.t()}
          | {:host, String.t()}
          | {:port, integer()}
          | {:ssl_options, ssl_options}
          | {:channels, integer()}

  @type mississippi_config ::
          {:events_exchange_name, String.t()}
          | {:data_queue_count, pos_integer()}
          | {:data_queue_prefix, String.t()}

  @type init_options :: [
          {:amqp_producer_options, amqp_options()}
          | {:mississippi_config, mississippi_config()}
          | {:events_producer_connection_number, pos_integer()}
        ]

  @spec start_link([init_options()]) :: Supervisor.on_start()
  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_opts) do
    opts = NimbleOptions.validate!(init_opts, Options.definition())

    # TODO: `events_producer_connection_number` should be automatically computed based on
    # `data_queue_count` + `channels_per_connection` (this one will arrive soon).
    events_producer_connection_number = opts[:events_producer_connection_number]

    children = [
      {ExRabbitPool.PoolSupervisor,
       rabbitmq_config: opts[:amqp_producer_options],
       connection_pools: [events_producer_pool_config(events_producer_connection_number)]},
      {EventsProducer, opts[:mississippi_config]}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Mississippi.Producer.Supervisor]
    Supervisor.init(children, opts)
  end

  defp events_producer_pool_config(events_producer_connection_number) do
    [
      name: {:local, :events_producer_pool},
      worker_module: ExRabbitPool.Worker.RabbitConnection,
      size: events_producer_connection_number,
      max_overflow: 0
    ]
  end
end
