# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.AMQPConnection do
  @moduledoc """
  Producer AMQP connection
  """

  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Exchange

  require Logger

  @doc """
  Initializes the connection. Links to the connection.
  """
  def init(connection_options, events_exchange_name) do
    with {:ok, conn} <- Connection.open(connection_options),
         true = Process.link(conn.pid),
         {:ok, channel} <- checkout_channel(conn),
         :ok <- declare_events_exchange(channel, events_exchange_name) do
      {:ok, channel}
    end
  end

  defp checkout_channel(conn) do
    with {:error, reason} <- Channel.open(conn) do
      Logger.warning("Failed to check out channel for producer: #{inspect(reason)}")

      {:error, :event_producer_channel_checkout_fail}
    end
  end

  defp declare_events_exchange(channel, events_exchange_name) do
    with {:error, reason} <- declare_exchange(channel, events_exchange_name) do
      Logger.warning("Error declaring EventsProducer default events exchange: #{inspect(reason)}")

      # Something went wrong
      close_channel(channel)

      {:error, :event_producer_init_fail}
    end
  end

  defp declare_exchange(_channel, ""), do: :ok

  defp declare_exchange(channel, exchange_name) do
    Exchange.declare(channel, exchange_name, :direct, durable: true)
  end

  @spec close_connection(Channel.t() | nil) :: :ok
  def close_connection(channel) do
    if channel && Process.alive?(channel.pid) do
      close_channel(channel)
    else
      :ok
    end
  end

  @doc false
  def close_channel(channel) do
    Channel.close(channel)
    Process.unlink(channel.conn.pid)
    Connection.close(channel.conn)
    :ok
  end
end
