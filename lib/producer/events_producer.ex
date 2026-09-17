# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer do
  @moduledoc """
  The entry point for publishing messages on Mississippi.

  Publish is now sharded: it hashes the `sharding_key` and routes the
  message to the per-queue `Worker` that owns the corresponding queue.
  """

  alias AMQP.Basic
  alias Horde.Registry
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.Options
  alias Mississippi.Producer.EventsProducer.Worker

  # API

  @doc """
  Publish a message on Mississippi AMQP queues. The call is blocking, as only one message at a time can be published on a given shard.
  """
  @type publish_opts() :: keyword()
  @type mississippi_config() :: keyword()
  @spec publish(payload :: binary(), publish_opts :: publish_opts()) ::
          :ok | {:error, :reconnecting, :events_producer_uninitialized} | Basic.error()
  def publish(payload, opts) do
    publish_opts = NimbleOptions.validate!(opts, Options.publish_opts())
    sharding_key = publish_opts[:sharding_key]

    case Registry.lookup(EventsProducer.Registry, :events_producer_config) do
      [{_pid, queues_config}] ->
        total_count = Keyword.fetch!(queues_config, :total_count)

        Worker.for_sharding_key(sharding_key, total_count)
        |> Worker.publish(payload, publish_opts)

      [] ->
        {:error, :events_producer_uninitialized}
    end
  end
end
