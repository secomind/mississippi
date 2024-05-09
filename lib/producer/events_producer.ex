defmodule Mississippi.Producer.EventsProducer do
  @moduledoc """
  The entry point for publishing messages on Mississippi.
  """

  defmodule State do
    defstruct [
      :channel,
      :events_exchange_name,
      :data_queue_count,
      :data_queue_prefix
    ]
  end

  use GenServer

  alias AMQP.Channel
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
  @type publish_opts :: [{:sharding_key, term()}] | [{:sharding_key, term()}, {:headers, list()}]
  @spec publish(
          payload :: binary(),
          opts :: publish_opts()
        ) :: :ok | {:error, reason :: :blocked | :closing}
  def publish(payload, opts \\ []) do
    GenServer.call(__MODULE__, {:publish, payload, opts})
  end

  # Server callbacks

  @impl true
  def init(queues_config: queues_config) do
    events_exchange_name = Keyword.fetch!(queues_config, :events_exchange_name)
    data_queue_count = Keyword.fetch!(queues_config, :data_queue_count)
    data_queue_prefix = Keyword.fetch!(queues_config, :data_queue_prefix)

    case init_producer(events_exchange_name) do
      {:ok, channel} ->
        {:ok,
         %State{
           channel: channel,
           events_exchange_name: events_exchange_name,
           data_queue_count: data_queue_count,
           data_queue_prefix: data_queue_prefix
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
      data_queue_count: data_queue_count,
      data_queue_prefix: data_queue_prefix
    } = state

    headers =
      Keyword.get(opts, :headers, [])
      |> Keyword.put(:sharding_key, sharding_key)

    # TODO: handle basic.return
    full_opts =
      opts
      |> Keyword.delete(:sharding_key)
      |> Keyword.put(:persistent, true)
      |> Keyword.put(:mandatory, true)
      |> Keyword.put(:headers, headers)
      |> Keyword.put(:message_id, generate_message_id())
      |> Keyword.put(:timestamp, DateTime.utc_now() |> DateTime.to_unix())

    queue_index = :erlang.phash2(sharding_key, data_queue_count)
    queue_name = "#{data_queue_prefix}#{queue_index}"
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
