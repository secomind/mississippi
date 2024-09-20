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
    children = amqp_data_consumers_childspecs(queues_config)

    Enum.each(children, fn child ->
      DynamicSupervisor.start_child(AMQPDataConsumer.Supervisor, child)
    end)
  end

  defp amqp_data_consumers_childspecs(queues_config) do
    queue_prefix = queues_config[:prefix]
    queue_total = queues_config[:total_count]
    max_index = queue_total - 1

    for queue_index <- 0..max_index do
      queue_name = "#{queue_prefix}#{queue_index}"

      init_args = [
        queue_name: queue_name,
        queue_index: queue_index
      ]

      {AMQPDataConsumer, init_args}
    end
  end
end
