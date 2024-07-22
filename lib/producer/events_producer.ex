defmodule Mississippi.Producer.EventsProducer do
  @moduledoc """
  The entry point for publishing messages on Mississippi.
  """

  use GenServer

  alias AMQP.Basic
  alias Mississippi.Producer.EventsProducer.Options
  alias Mississippi.Producer.EventsProducer.State
  require Logger

  # TODO should these be customizable?
  @connection_backoff :timer.seconds(10)
  @adapter ExRabbitPool.RabbitMQ

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

    state = %State{
      channel: nil,
      events_exchange_name: events_exchange_name,
      queue_total_count: queue_count,
      queue_prefix: queue_prefix
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
      Keyword.get(opts, :headers, [])
      |> Keyword.put(:sharding_key, :erlang.term_to_binary(sharding_key))

    # TODO: handle basic.return
    full_opts =
      opts
      |> Keyword.delete(:sharding_key)
      |> Keyword.put(:persistent, true)
      |> Keyword.put(:mandatory, true)
      |> Keyword.put(:headers, headers)
      |> Keyword.put_new(:message_id, generate_message_id())
      |> Keyword.put_new(:timestamp, DateTime.utc_now() |> DateTime.to_unix())

    queue_index = :erlang.phash2(sharding_key, queue_count)
    queue_name = "#{queue_prefix}#{queue_index}"
    # TODO should the producer really declare the queue? Nvm for now, it's idempotent
    {:ok, _} = @adapter.declare_queue(channel, queue_name, durable: true)
    res = @adapter.publish(channel, events_exchange_name, queue_name, payload, full_opts)
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
    case init_producer_channel(state) do
      {:ok, channel} ->
        Process.monitor(channel.pid)

        Logger.debug("EventsProducer initialized")

        %State{state | channel: channel}

      {:error, _reason} ->
        schedule_connect()
        %State{state | channel: nil}
    end
  end

  defp init_producer_channel(state) do
    conn = ExRabbitPool.get_connection_worker(:events_producer_pool)

    with {:ok, channel} <- checkout_channel(conn),
         :ok <- declare_events_exchange(conn, channel, state.events_exchange_name) do
      {:ok, channel}
    end
  end

  defp checkout_channel(conn) do
    with {:error, reason} <- ExRabbitPool.checkout_channel(conn) do
      _ =
        Logger.warning("Failed to check out channel for producer: #{inspect(reason)}")

      {:error, :event_producer_channel_checkout_fail}
    end
  end

  defp declare_events_exchange(conn, channel, events_exchange_name) do
    with {:error, reason} <-
           @adapter.declare_exchange(channel, events_exchange_name,
             type: :direct,
             durable: true
           ) do
      Logger.warning("Error declaring EventsProducer default events exchange: #{inspect(reason)}")

      # Something went wrong, let's put the channel back where it belongs
      _ = ExRabbitPool.checkin_channel(conn, channel)
      {:error, :event_producer_init_fail}
    end
  end

  defp schedule_connect() do
    _ = Logger.warning("Retrying connection in #{@connection_backoff} ms")
    Process.send_after(self(), :init_producer, @connection_backoff)
  end

  defp generate_message_id() do
    UUID.uuid4()
  end
end
