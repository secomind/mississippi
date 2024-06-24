defmodule Mississippi.Producer.EventsProducer do
  @moduledoc """
  The entry point for publishing messages on Mississippi.
  """

  defmodule State do
    defstruct [
      :channel,
      :events_exchange_name,
      :queue_total_count,
      :queue_prefix
    ]
  end

  use GenServer

  alias AMQP.Channel
  alias Mississippi.Producer.EventsProducer.Options
  require Logger

  # TODO should these be customizable?
  @connection_backoff 10000
  @adapter ExRabbitPool.RabbitMQ

  # API

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @doc """
  Publish a message on Mississippi AMQP queues. The call is blocking, as only one message at a time can be published.
  """
  @type publish_opts() :: [unquote(NimbleOptions.option_typespec(Options.publish_opts()))]
  @spec publish(
          payload :: binary(),
          opts :: publish_opts()
        ) :: :ok | {:error, reason :: :blocked | :closing}
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

    case init_producer(events_exchange_name) do
      {:ok, channel} ->
        {:ok,
         %State{
           channel: channel,
           events_exchange_name: events_exchange_name,
           queue_total_count: queue_count,
           queue_prefix: queue_prefix
         }}

      {:error, reason} ->
        {:stop, reason}
    end
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
  def handle_info({:DOWN, _, :process, _pid, reason}, state) do
    Logger.warning("RabbitMQ connection lost: #{inspect(reason)}. Trying to reconnect...",
      tag: "events_producer_conn_lost"
    )

    case init_producer(state.events_exchange_name) do
      {:ok, channel} ->
        {:noreply, channel}

      {:error, _reason} ->
        schedule_connect()
        {:noreply, :not_connected}
    end
  end

  defp init_producer(events_exchange_name) do
    conn = ExRabbitPool.get_connection_worker(:events_producer_pool)

    with {:ok, channel} <- checkout_channel(conn),
         :ok <- declare_events_exchange(conn, channel, events_exchange_name) do
      %Channel{pid: channel_pid} = channel
      _ref = Process.monitor(channel_pid)

      _ =
        Logger.debug("EventsProducer initialized",
          tag: "event_producer_init_ok"
        )

      {:ok, channel}
    end
  end

  defp checkout_channel(conn) do
    with {:error, reason} <- ExRabbitPool.checkout_channel(conn) do
      _ =
        Logger.warning(
          "Failed to check out channel for producer: #{inspect(reason)}",
          tag: "event_producer_channel_checkout_fail"
        )

      {:error, :event_producer_channel_checkout_fail}
    end
  end

  defp declare_events_exchange(conn, channel, events_exchange_name) do
    with {:error, reason} <-
           @adapter.declare_exchange(channel, events_exchange_name,
             type: :direct,
             durable: true
           ) do
      Logger.warning(
        "Error declaring EventsProducer default events exchange: #{inspect(reason)}",
        tag: "event_producer_init_fail"
      )

      # Something went wrong, let's put the channel back where it belongs
      _ = ExRabbitPool.checkin_channel(conn, channel)
      {:error, :event_producer_init_fail}
    end
  end

  defp schedule_connect() do
    _ = Logger.warning("Retrying connection in #{@connection_backoff} ms")
    Process.send_after(@connection_backoff, self(), :init)
  end

  defp generate_message_id() do
    UUID.uuid4()
  end
end
