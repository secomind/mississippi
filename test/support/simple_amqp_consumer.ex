defmodule SimpleAMQPConsumer do
  @moduledoc false
  use GenServer

  @impl true
  def init(init_arg) do
    queue_name = Keyword.fetch!(init_arg, :queue_name)
    receiver = Keyword.fetch!(init_arg, :receiver)

    {:ok, %{queue_name: queue_name, receiver: receiver}, {:continue, :init_consume}}
  end

  @impl true
  def handle_continue(:init_consume, state) do
    conn = ExRabbitPool.get_connection_worker(:events_consumer_pool)

    with {:ok, channel} <- ExRabbitPool.checkout_channel(conn),
         :ok <- AMQP.Basic.qos(channel),
         {:ok, _queue} <- AMQP.Queue.declare(channel, state.queue_name, durable: true),
         # TODO use receiver rather than self()?
         {:ok, _consumer_tag} <- AMQP.Basic.consume(channel, state.queue_name, self()) do
      {:noreply, state}
    end
  end

  # Message consumed
  @impl true
  def handle_info({:basic_deliver, payload, meta}, state) do
    {headers, no_headers_meta} = Map.pop(meta, :headers, [])
    headers_map = amqp_headers_to_map(headers)

    {timestamp, _clean_meta} = Map.pop(no_headers_meta, :timestamp)

    Process.send(state.receiver, {payload, headers_map, timestamp}, [])
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _}, state) do
    {:noreply, state}
  end

  defp amqp_headers_to_map(headers) do
    Enum.reduce(headers, %{}, fn {key, _type, value}, acc ->
      Map.put(acc, key, value)
    end)
  end
end
