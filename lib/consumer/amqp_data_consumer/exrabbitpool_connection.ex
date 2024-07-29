defmodule Mississippi.Consumer.AMQPDataConsumer.ExRabbitPoolConnection do
  @moduledoc false
  @behaviour Mississippi.Consumer.AMQPDataConsumer.AMQPConnection

  alias Mississippi.Consumer.AMQPDataConsumer.State

  require Logger

  @consumer_prefetch_count 300

  def adapter, do: ExRabbitPool.RabbitMQ

  def init(state) do
    conn = ExRabbitPool.get_connection_worker(:events_consumer_pool)

    case ExRabbitPool.checkout_channel(conn) do
      {:ok, channel} ->
        try_to_setup_consume(channel, conn, state)

      {:error, reason} ->
        _ =
          Logger.warning("Failed to check out channel for consumer on queue #{state.queue_name}: #{inspect(reason)}")

        {:error, reason}
    end
  end

  defp try_to_setup_consume(channel, conn, state) do
    %State{queue_name: queue_name} = state

    with :ok <- adapter().qos(channel, prefetch_count: @consumer_prefetch_count),
         {:ok, _queue} <- adapter().declare_queue(channel, queue_name, durable: true),
         {:ok, _consumer_tag} <- adapter().consume(channel, queue_name, self()) do
      {:ok, channel}
    else
      {:error, reason} ->
        Logger.warning("Error initializing AMQPDataConsumer on queue #{state.queue_name}: #{inspect(reason)}")

        # Something went wrong, let's put the channel back where it belongs
        _ = ExRabbitPool.checkin_channel(conn, channel)
        {:error, reason}
    end
  end
end
