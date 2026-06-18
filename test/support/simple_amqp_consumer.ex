# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule SimpleAMQPConsumer do
  @moduledoc false
  use GenServer

  alias Mississippi.Consumer.AMQPDataConsumer.AMQPConnection

  def start_link(init_arg), do: GenServer.start_link(__MODULE__, init_arg)

  def consumer_ready_message(exchange_name, queue_name), do: {:consumer_ready, exchange_name, queue_name}

  @impl true
  def init(init_arg) do
    queue_name = Keyword.fetch!(init_arg, :queue_name)
    receiver = Keyword.fetch!(init_arg, :receiver)
    consumer_options = Keyword.fetch!(init_arg, :consumer_options)
    exchange = Keyword.fetch!(init_arg, :exchange)

    state = %{
      queue_name: queue_name,
      receiver: receiver,
      consumer_options: consumer_options,
      exchange: exchange
    }

    {:ok, state, {:continue, :init_consume}}
  end

  @impl true
  def handle_continue(:init_consume, state) do
    with {:ok, _channel} <-
           AMQPConnection.init(state.consumer_options, state.exchange, state.queue_name) do
      {:noreply, state}
    end
  end

  # Message consumed
  @impl true
  def handle_info({:basic_deliver, payload, meta}, state) do
    clean_meta = Map.reject(meta, fn {_key, value} -> value == :undefined end)
    {headers, no_headers_meta} = Map.pop(clean_meta, :headers, [])
    headers_map = amqp_headers_to_map(headers)

    {timestamp, _clean_meta} = Map.pop(no_headers_meta, :timestamp)

    Process.send(state.receiver, {payload, headers_map, timestamp}, [])
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _}, state) do
    message = consumer_ready_message(state.exchange, state.queue_name)
    send(state.receiver, message)

    {:noreply, state}
  end

  defp amqp_headers_to_map(headers) do
    Enum.reduce(headers, %{}, fn {key, _type, value}, acc ->
      Map.put(acc, key, value)
    end)
  end
end
