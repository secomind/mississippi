defmodule Mississippi.Consumer.AMQPDataConsumer.Supervisor do
  use Supervisor

  alias Mississippi.Consumer.AMQPDataConsumer

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    children =
      amqp_data_consumers_childspecs(init_arg[:queue_config], init_arg[:message_handler])

    opts = [strategy: :one_for_one, name: __MODULE__]

    Supervisor.init(children, opts)
  end

  defp amqp_data_consumers_childspecs(queue_config, message_handler) do
    queue_range_start = queue_config[:range_start]
    queue_range_end = queue_config[:range_end]
    queue_prefix = queue_config[:prefix]

    for queue_index <- queue_range_start..queue_range_end do
      queue_name = "#{queue_prefix}#{queue_index}"

      init_args = [
        queue_name: queue_name,
        queue_index: queue_index,
        range_start: queue_range_start,
        range_end: queue_range_end,
        queue_total_count: queue_config[:total_count],
        message_handler: message_handler
      ]

      Supervisor.child_spec({AMQPDataConsumer, init_args}, id: {AMQPDataConsumer, queue_index})
    end
  end
end
