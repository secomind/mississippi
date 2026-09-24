# Copyright 2025 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.NodeListener do
  @moduledoc false
  use GenServer

  alias Mississippi.Producer.EventsProducer.Starter

  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def init(queues_config) do
    with {:ok, starter_pid} <- Starter.start_link(queues_config) do
      :net_kernel.monitor_nodes(true, node_type: :visible)
      {:ok, starter_pid}
    end
  end

  def handle_info({:nodeup, node, node_type}, starter) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is up")
    _ = Starter.start_producers(starter)
    {:noreply, starter}
  end

  def handle_info({:nodedown, node, node_type}, starter) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is down")
    _ = Starter.start_producers(starter)
    {:noreply, starter}
  end
end
