defmodule Mississippi.Consumer do
  @moduledoc """
  This module defines the supervision tree of Mississippi.Consumer.
  """
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

  @type mississippi_queues_config ::
          {:events_exchange_name, String.t()}
          | {:data_queue_range_start, non_neg_integer()}
          | {:data_queue_range_end, pos_integer()}
          | {:data_queue_total_count, pos_integer()}
          | {:data_queue_prefix, String.t()}

  @type init_options :: [
          {:amqp_consumer_options, amqp_options()}
          | {:mississippi_queues_config, mississippi_queues_config()}
          | {:events_consumer_connection_number, pos_integer()}
          | {:message_handler, module()}
        ]

  @spec start_link(init_options()) :: Supervisor.on_start()
  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    # TODO: `events_consumer_connection_number` should be automatically computed based on
    # `data_queue_range_start` and `data_queue_range_end` + `channels_per_connections` (this one will arrive soon).
    children = [
      {Mississippi.Consumer.DataPipelineSupervisor, init_arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Mississippi.Consumer.Supervisor]
    Supervisor.init(children, opts)
  end
end
