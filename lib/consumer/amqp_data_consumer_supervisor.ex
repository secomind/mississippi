defmodule Mississippi.Consumer.AMQPDataConsumer.Supervisor do
  @moduledoc false
  use Supervisor

  alias Mississippi.Consumer.AMQPDataConsumer

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    children =
      amqp_data_consumers_childspecs(init_arg[:queues_config])

    opts = [strategy: :one_for_one]

    Supervisor.init(children, opts)
  end

  defp amqp_data_consumers_childspecs(queues_config) do
    queue_range_start = queues_config[:range_start]
    queue_range_end = queues_config[:range_end]
    queue_prefix = queues_config[:prefix]

    for queue_index <- queue_range_start..queue_range_end do
      queue_name = "#{queue_prefix}#{queue_index}"

      init_args = [
        queue_name: queue_name,
        queue_index: queue_index
      ]

      Supervisor.child_spec({AMQPDataConsumer, init_args}, id: {AMQPDataConsumer, queue_index})
    end
  end
end
