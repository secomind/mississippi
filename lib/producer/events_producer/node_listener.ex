# Copyright 2025 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.NodeListener do
  @moduledoc false
  use GenServer

  alias Mississippi.Producer.EventsProducer.Starter

  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def init(init_args) do
    queues_config = Keyword.fetch!(init_args, :queues_config)
    worker_distribution_scope = Keyword.get(init_args, :worker_distribution_scope, :cluster)

    with {:ok, starter_pid} <- Starter.start_link(queues_config) do
      if worker_distribution_scope == :cluster do
        # listen and react to node topology changes only if in cluster mode
        :net_kernel.monitor_nodes(true, node_type: :visible)
      end

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
