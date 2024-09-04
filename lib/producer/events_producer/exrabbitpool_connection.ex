# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.ExRabbitPoolConnection do
  @moduledoc false
  @behaviour Mississippi.AMQPConnection

  require Logger

  def adapter, do: ExRabbitPool.RabbitMQ

  def init(state) do
    conn = ExRabbitPool.get_connection_worker(:events_producer_pool)

    with {:ok, channel} <- checkout_channel(conn),
         :ok <- declare_events_exchange(conn, channel, state.events_exchange_name) do
      {:ok, channel}
    end
  end

  defp checkout_channel(conn) do
    with {:error, reason} <- ExRabbitPool.checkout_channel(conn) do
      Logger.warning("Failed to check out channel for producer: #{inspect(reason)}")

      {:error, :event_producer_channel_checkout_fail}
    end
  end

  defp declare_events_exchange(conn, channel, events_exchange_name) do
    with {:error, reason} <-
           adapter().declare_exchange(channel, events_exchange_name, type: :direct, durable: true) do
      Logger.warning("Error declaring EventsProducer default events exchange: #{inspect(reason)}")

      # Something went wrong, let's put the channel back where it belongs
      _ = ExRabbitPool.checkin_channel(conn, channel)
      {:error, :event_producer_init_fail}
    end
  end
end
