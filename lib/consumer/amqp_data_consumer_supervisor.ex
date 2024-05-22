defmodule Mississippi.Consumer.AMQPDataConsumer.Supervisor do
  use Supervisor

  alias Mississippi.Consumer.AMQPDataConsumer

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    data_queues_config = Keyword.fetch!(init_arg, :mississippi_queues_config)
    message_handler = Keyword.fetch!(init_arg, :message_handler)

    children =
      amqp_data_consumers_childspecs(data_queues_config, message_handler)

    opts = [strategy: :one_for_one, name: __MODULE__]

    Supervisor.init(children, opts)
  end

  defp amqp_data_consumers_childspecs(data_queues_config, message_handler) do
    queue_range_start = Keyword.fetch!(data_queues_config, :data_queue_range_start)
    queue_range_end = Keyword.fetch!(data_queues_config, :data_queue_range_end)
    queue_prefix = Keyword.fetch!(data_queues_config, :data_queue_prefix)
    queue_total_count = Keyword.fetch!(data_queues_config, :data_queue_total_count)

    for queue_index <- queue_range_start..queue_range_end do
      queue_name = "#{queue_prefix}#{queue_index}"

      init_args = [
        queue_name: queue_name,
        queue_index: queue_index,
        data_queue_range_start: queue_range_start,
        data_queue_range_end: queue_range_end,
        data_queue_total_count: queue_total_count,
        message_handler: message_handler
      ]

      Supervisor.child_spec({AMQPDataConsumer, init_args}, id: {AMQPDataConsumer, queue_index})
    end
  end
end
