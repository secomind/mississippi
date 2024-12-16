defmodule NodeListener do
  @moduledoc false
  use GenServer

  alias Mississippi.Consumer.AMQPDataConsumer

  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def init(queues_config) do
    :net_kernel.monitor_nodes(true, node_type: :visible)
    {:ok, queues_config}
  end

  def handle_info({:nodeup, node, node_type}, queues_config) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is up")
    _ = AMQPDataConsumer.Supervisor.start_consumers(queues_config)
    {:noreply, queues_config}
  end

  def handle_info({:nodedown, node, node_type}, queues_config) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is down")
    _ = AMQPDataConsumer.Supervisor.start_consumers(queues_config)
    {:noreply, queues_config}
  end
end
