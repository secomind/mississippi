# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.Worker do
  @moduledoc """
  A per-queue producer worker that owns a single AMQP channel and declares its queue once at startup.
  Mimics Mississippi.Consumer.AMQPDataConsumer's channel handling (trap_exit + link).
  """

  use GenServer, restart: :transient

  alias AMQP.Basic
  alias Mississippi.Producer.EventsProducer.ExRabbitPoolConnection
  alias Mississippi.Producer.EventsProducer.Options
  alias Mississippi.Producer.EventsProducer.State

  require Logger

  # API

  def start_link(args) do
    index = Keyword.fetch!(args, :queue_index)
    GenServer.start_link(__MODULE__, args, name: via_tuple(index))
  end

  def via_tuple(queue_index) when is_integer(queue_index) do
    {:via, Horde.Registry,
     {Mississippi.Producer.EventsProducer.Registry, {:queue_index, queue_index}}}
  end

  @doc """
  Returns the worker for the given sharding key
  """
  def for_sharding_key(sharding_key, total_queue_count) do
    queue_index = :erlang.phash2(sharding_key, total_queue_count)

    via_tuple(queue_index)
  end

  @doc """
  Publish via the worker for its queue. Used internally by EventsProducer.publish/2.
  """
  @spec publish(pid() | tuple(), binary(), keyword()) :: :ok | {:error, term()} | Basic.error()
  def publish(server, payload, opts) do
    GenServer.call(server, {:publish, payload, opts})
  end

  @doc """
  Gets the status of the AMQP channel linked to the worker (:up/:down).
  """
  def get_amqp_channel_status(server) do
    GenServer.call(server, :get_amqp_channel_status)
  end

  # Server callbacks

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)

    queue_name = Keyword.fetch!(args, :queue_name)
    queue_index = Keyword.fetch!(args, :queue_index)
    events_exchange_name = Keyword.fetch!(args, :events_exchange_name)
    connection = Keyword.get(args, :connection, ExRabbitPoolConnection)
    reconnection_backoff_ms = Keyword.get(args, :reconnection_backoff_ms, 1_000)

    state = %State{
      queue_name: queue_name,
      queue_index: queue_index,
      events_exchange_name: events_exchange_name,
      channel: nil,
      connection: connection,
      reconnection_backoff_ms: reconnection_backoff_ms
    }

    {:ok, state, {:continue, :init_producer}}
  end

  @impl true
  def handle_continue(:init_producer, state), do: {:noreply, init_producer(state)}

  @impl true
  def handle_call({:publish, _, _}, _from, %State{channel: nil} = state) do
    {:reply, {:error, :reconnecting}, state}
  end

  @impl true
  def handle_call({:publish, payload, opts}, _from, state) do
    headers =
      opts
      |> Keyword.get(:headers, [])
      |> Keyword.put(:sharding_key, :erlang.term_to_binary(Keyword.fetch!(opts, :sharding_key)))

    full_opts =
      opts
      |> Keyword.delete(:sharding_key)
      |> Keyword.put(:persistent, true)
      |> Keyword.put(:mandatory, true)
      |> Keyword.put(:headers, headers)
      |> Keyword.put_new(:message_id, generate_message_id())
      |> Keyword.put_new(:timestamp, DateTime.to_unix(DateTime.utc_now()))

    %State{
      channel: channel,
      events_exchange_name: events_exchange_name,
      queue_name: queue_name
    } = state

    res =
      state.connection.adapter().publish(
        channel,
        events_exchange_name,
        queue_name,
        payload,
        full_opts
      )

    {:reply, res, state}
  end

  @impl true
  def handle_call(:get_amqp_channel_status, _from, %State{channel: channel} = state) do
    channel_status =
      case channel do
        %AMQP.Channel{} ->
          if amqp_channel_up?(channel), do: :up, else: :down

        _ ->
          :down
      end

    {:reply, channel_status, state}
  end

  @impl true
  def handle_info(:init_producer, state), do: {:noreply, init_producer(state)}

  def handle_info({:EXIT, _from, {:name_conflict, {_key, _value}, _registry, _pid}}, state) do
    _ = Logger.warning("Duplicate EventsProducer.Worker shutting down")
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _from, {:shutdown, :process_redistribution}}, state) do
    _ = Logger.info("EventsProducer.Worker shutting down due to process redistribution")
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _from, reason}, state) do
    {:stop, reason, state}
  end

  defp amqp_channel_up?(%AMQP.Channel{
         pid: channel_pid,
         conn: %AMQP.Connection{pid: connection_pid}
       })
       when is_pid(channel_pid) and is_pid(connection_pid) do
    Process.alive?(channel_pid) and Process.alive?(connection_pid)
  end

  defp amqp_channel_up?(_channel), do: false

  defp init_producer(state) do
    %{connection: connection, queue_name: queue_name} = state

    with {:ok, channel} <- connection.init(state),
         _ = Process.link(channel.pid),
         {:ok, _queue} <- connection.adapter().declare_queue(channel, queue_name, durable: true) do
      Logger.debug("EventsProducer for queue #{state.queue_name} initialized")

      %State{state | channel: channel}
    else
      {:error, _reason} ->
        schedule_connect(state.reconnection_backoff_ms)
        %State{state | channel: nil}
    end
  end

  defp schedule_connect(backoff) do
    Process.send_after(self(), :init_producer, backoff)
  end

  defp generate_message_id do
    UUID.uuid4()
  end

  # NimbleOptions for validation is in Options module; we reuse it via router but keep helper if needed
  def publish_opts, do: Options.publish_opts()
end
