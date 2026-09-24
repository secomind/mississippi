# Copyright 2025 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.Starter do
  @moduledoc false
  use GenServer

  alias Horde.DynamicSupervisor
  alias Horde.Registry
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.Starter
  alias Mississippi.Producer.EventsProducer.Worker

  require Logger

  @restart_backoff :timer.seconds(2)

  def start_link(queues_config) do
    GenServer.start_link(Starter, queues_config)
  end

  def start_producers(starter), do: GenServer.call(starter, :start_producers)

  @impl GenServer
  def init(queues_config) do
    with :ok <- start_workers(queues_config),
         :ok <- store_config(queues_config) do
      {:ok, queues_config}
    end
  end

  @impl GenServer
  def handle_call(:start_producers, _from, queues_config) do
    {:reply, start_workers(queues_config), queues_config}
  end

  defp start_workers(queues_config) do
    retry(10, :cannot_start_producers, fn -> do_start_workers(queues_config) end)
  end

  defp do_start_workers(queues_config) do
    start_amqp_producers(queues_config)

    queue_total = queues_config[:total_count]

    child_count =
      Worker.Supervisor |> DynamicSupervisor.which_children() |> Enum.count()

    case child_count do
      ^queue_total ->
        true

      _ ->
        false
    end
  end

  def start_amqp_producers(queues_config) do
    children = amqp_producers_childspecs(queues_config)

    Enum.each(children, fn child ->
      DynamicSupervisor.start_child(Worker.Supervisor, child)
    end)
  end

  defp amqp_producers_childspecs(queues_config) do
    queue_prefix = queues_config[:prefix]
    queue_total = queues_config[:total_count]
    events_exchange_name = queues_config[:events_exchange_name]
    connection_options = queues_config[:connection_options]
    reconnection_backoff_ms = Keyword.get(queues_config, :reconnection_backoff_ms, 1_000)

    max_index = queue_total - 1

    for queue_index <- 0..max_index do
      routing_key = "#{queue_prefix}#{queue_index}"

      init_args = [
        routing_key: routing_key,
        queue_index: queue_index,
        events_exchange_name: events_exchange_name,
        connection_options: connection_options,
        reconnection_backoff_ms: reconnection_backoff_ms
      ]

      {Worker, init_args}
    end
  end

  @doc false
  def store_config(queues_config) do
    retry(10, :cannot_store_config, fn -> do_store_config(queues_config) end)
  end

  defp do_store_config(queues_config) do
    register = Registry.register(EventsProducer.Registry, :events_producer_config, queues_config)
    match?({:ok, _}, register)
  end

  defp retry(count, error_reason, fun) do
    case {count, fun.()} do
      {_, true} ->
        :ok

      {0, false} ->
        Logger.warning("Events Producer start failsed: #{error_reason}")
        {:error, error_reason}

      {n, false} ->
        backoff_delta = :rand.uniform(@restart_backoff)
        Process.sleep(@restart_backoff + backoff_delta)
        retry(n - 1, error_reason, fun)
    end
  end
end
