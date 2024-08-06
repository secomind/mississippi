defmodule Mississippi.Producer.EventsProducer do
  @moduledoc """
  The entry point for publishing messages on Mississippi.
  """

  use GenServer

  alias AMQP.Basic
  alias Mississippi.Producer.EventsProducer.ExRabbitPoolConnection
  alias Mississippi.Producer.EventsProducer.Options
  alias Mississippi.Producer.EventsProducer.State

  require Logger

  # API

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @doc """
  Publish a message on Mississippi AMQP queues. The call is blocking, as only one message at a time can be published.
  """
  @type publish_opts() :: [unquote(NimbleOptions.option_typespec(Options.publish_opts()))]
  @spec publish(payload :: binary(), opts :: publish_opts()) ::
          :ok | {:error, :reconnecting} | Basic.error()
  def publish(payload, opts) do
    valid_opts = NimbleOptions.validate!(opts, Options.publish_opts())
    GenServer.call(__MODULE__, {:publish, payload, valid_opts})
  end

  # Server callbacks

  @impl true
  def init(init_opts) do
    events_exchange_name = init_opts[:events_exchange_name]
    queue_count = init_opts[:total_count]
    queue_prefix = init_opts[:prefix]
    connection = Keyword.get(init_opts, :connection, ExRabbitPoolConnection)
    reconnection_backoff_ms = Keyword.get(init_opts, :reconnection_backoff_ms, :timer.seconds(10))

    state = %State{
      channel: nil,
      events_exchange_name: events_exchange_name,
      queue_total_count: queue_count,
      queue_prefix: queue_prefix,
      connection: connection,
      reconnection_backoff_ms: reconnection_backoff_ms
    }

    {:ok, init_producer(state)}
  end

  @impl true
  def handle_call({:publish, _, _}, _from, %State{channel: nil} = state) do
    # We're currently in reconnecting state
    {:reply, {:error, :reconnecting}, state}
  end

  @impl true
  def handle_call({:publish, payload, opts}, _from, state) do
    sharding_key = Keyword.fetch!(opts, :sharding_key)

    %State{
      channel: channel,
      events_exchange_name: events_exchange_name,
      queue_total_count: queue_count,
      queue_prefix: queue_prefix
    } = state

    headers =
      opts
      |> Keyword.get(:headers, [])
      |> Keyword.put(:sharding_key, :erlang.term_to_binary(sharding_key))

    # TODO: handle basic.return
    full_opts =
      opts
      |> Keyword.delete(:sharding_key)
      |> Keyword.put(:persistent, true)
      |> Keyword.put(:mandatory, true)
      |> Keyword.put(:headers, headers)
      |> Keyword.put_new(:message_id, generate_message_id())
      |> Keyword.put_new(:timestamp, DateTime.to_unix(DateTime.utc_now()))

    queue_index = :erlang.phash2(sharding_key, queue_count)
    queue_name = "#{queue_prefix}#{queue_index}"
    # TODO should the producer really declare the queue? Nvm for now, it's idempotent
    {:ok, _} = state.connection.adapter().declare_queue(channel, queue_name, durable: true)

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
  def handle_info(:init_producer, state), do: {:noreply, init_producer(state)}

  @impl true
  def handle_info({:DOWN, _, :process, _pid, reason}, state) do
    Logger.warning("RabbitMQ connection lost: #{inspect(reason)}. Trying to reconnect...")

    {:noreply, init_producer(state)}
  end

  defp init_producer(state) do
    case state.connection.init(state) do
      {:ok, channel} ->
        Process.monitor(channel.pid)

        Logger.debug("EventsProducer initialized")

        %State{state | channel: channel}

      {:error, _reason} ->
        schedule_connect(state.reconnection_backoff_ms)
        %State{state | channel: nil}
    end
  end

  defp schedule_connect(backoff) do
    _ = Logger.warning("Retrying connection in #{backoff} ms")
    Process.send_after(self(), :init_producer, backoff)
  end

  defp generate_message_id do
    UUID.uuid4()
  end
end
