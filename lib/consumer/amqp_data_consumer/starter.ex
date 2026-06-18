# Copyright 2025 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.AMQPDataConsumer.Starter do
  @moduledoc false
  use Task, restart: :transient

  alias Horde.DynamicSupervisor
  alias Mississippi.Consumer.AMQPDataConsumer

  require Logger

  @restart_backoff :timer.seconds(2)

  def start_link(queues_config) do
    Task.start_link(__MODULE__, :start_consumers, [queues_config])
  end

  def start_consumers(queues_config) do
    start_consumers(queues_config, 10)
  end

  defp start_consumers(_, 0) do
    _ = Logger.warning("Cannot start AMQPDataConsumers")
    {:error, :cannot_start_consumers}
  end

  defp start_consumers(queues_config, retry) do
    start_amqp_consumers(queues_config)

    queue_total = queues_config[:total_count]

    child_count =
      AMQPDataConsumer.Supervisor |> DynamicSupervisor.which_children() |> Enum.count()

    case child_count do
      ^queue_total ->
        :ok

      _ ->
        # TODO: do we want something more refined, e.g. exponential backoff?
        backoff_delta = :rand.uniform(@restart_backoff)
        Process.sleep(@restart_backoff + backoff_delta)
        start_consumers(queues_config, retry - 1)
    end
  end

  def start_amqp_consumers(queues_config) do
    queues = amqp_queues(queues_config)
    start_children(queues, queues_config)

    wait_children(queues)
  end

  defp start_children(queues, queues_config) do
    children = amqp_data_consumers_childspecs(queues, queues_config)

    Enum.each(children, fn child ->
      DynamicSupervisor.start_child(AMQPDataConsumer.Supervisor, child)
    end)
  end

  defp wait_children(queues) do
    for {_, queue_name} <- queues do
      barrier = AMQPDataConsumer.barrier(queue_name)

      receive do
        ^barrier -> :ok
      after
        1000 -> raise "Queue #{queue_name} did not start successfully"
      end
    end
  end

  defp amqp_queues(queues_config) do
    queue_prefix = queues_config[:prefix]
    queue_total = queues_config[:total_count]
    max_index = queue_total - 1

    for queue_index <- 0..max_index do
      queue_name = "#{queue_prefix}#{queue_index}"
      {queue_index, queue_name}
    end
  end

  defp amqp_data_consumers_childspecs(queues, queues_config) do
    connection_options = queues_config[:amqp_consumer_options]
    exchange_name = queues_config[:events_exchange_name]
    orchestrator = self()

    for {queue_index, queue_name} <- queues do
      init_args =
        [
          queue_name: queue_name,
          queue_index: queue_index,
          exchange_name: exchange_name,
          connection_options: connection_options,
          orchestrator: orchestrator
        ]

      {AMQPDataConsumer, init_args}
    end
  end
end
