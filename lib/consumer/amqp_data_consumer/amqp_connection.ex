# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.AMQPDataConsumer.AMQPConnection do
  @moduledoc """
  Consumer AMQP connection
  """

  alias AMQP.Basic
  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Queue

  require Logger

  @consumer_prefetch_count 300

  @doc """
  Initializes the connection. Links the connection and the channel pids.
  """
  def init(connection_options, exchange_name, queue_name) do
    with {:ok, conn} <- Connection.open(connection_options),
         true = Process.link(conn.pid),
         {:ok, channel} <- Channel.open(conn),
         true = Process.link(channel.pid),
         :ok <- Basic.qos(channel, prefetch_count: @consumer_prefetch_count),
         :ok <- declare_queue(channel, queue_name, durable: true),
         :ok <- bind_queue(channel, queue_name, exchange_name),
         {:ok, _consumer_tag} <- Basic.consume(channel, queue_name, self()) do
      {:ok, channel}
    else
      {:error, reason} ->
        Logger.warning("Error initializing AMQPDataConsumer on queue #{queue_name}: #{inspect(reason)}")

        {:error, reason}
    end
  end

  defp declare_queue(channel, queue, opts) do
    case Queue.declare(channel, queue, opts) do
      :ok -> :ok
      {:ok, _config} -> :ok
      error -> error
    end
  end

  defp bind_queue(_channel, _queue_name, ""), do: :ok

  defp bind_queue(channel, queue_name, exchange_name) do
    Queue.bind(channel, queue_name, exchange_name, routing_key: queue_name)
  end
end
