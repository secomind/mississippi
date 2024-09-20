defmodule Mississippi.Consumer.AMQPDataConsumer.Supervisor do
  @moduledoc false
  use Horde.DynamicSupervisor

  alias Horde.DynamicSupervisor
  alias Mississippi.Consumer.AMQPDataConsumer

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg,
      name: __MODULE__,
      distribution_strategy: Horde.UniformQuorumDistribution
    )
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(
      members: :auto,
      strategy: :one_for_one,
      process_redistribution: :active
    )
  end

  def start_consumers(queues_config) do
    children = amqp_data_consumers_childspecs(queues_config)

    Enum.each(children, fn child ->
      DynamicSupervisor.start_child(Mississippi.Consumer.AMQPDataConsumer.Supervisor, child)
    end)
  end

  defp amqp_data_consumers_childspecs(queues_config) do
    queue_total = queues_config[:total_count]
    queue_prefix = queues_config[:prefix]

    for queue_index <- 0..(queue_total - 1) do
      queue_name = "#{queue_prefix}#{queue_index}"

      init_args = [
        queue_name: queue_name,
        queue_index: queue_index
      ]

      Supervisor.child_spec({AMQPDataConsumer, init_args}, id: {AMQPDataConsumer, queue_index})
    end
  end
end
