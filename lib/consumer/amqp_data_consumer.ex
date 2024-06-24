defmodule Mississippi.Consumer.AMQPDataConsumer do
  defmodule State do
    defstruct [
      :channel,
      :monitor,
      :queue_name,
      :queue_range,
      :queue_total_count,
      :message_handler
    ]
  end

  require Logger
  use GenServer

  alias AMQP.Channel
  alias Mississippi.Consumer.DataUpdater

  # TODO should this be customizable?
  @reconnect_interval 1_000
  @adapter ExRabbitPool.RabbitMQ
  @sharding_key "sharding_key"
  @consumer_prefetch_count 300

  # API

  def start_link(args) do
    index = Keyword.fetch!(args, :queue_index)
    GenServer.start_link(__MODULE__, args, name: get_queue_via_tuple(index))
  end

  def ack(pid, delivery_tag) do
    Logger.debug("Going to ack #{inspect(delivery_tag)}")
    GenServer.call(pid, {:ack, delivery_tag})
  end

  def discard(pid, delivery_tag) do
    Logger.debug("Going to discard #{inspect(delivery_tag)}")
    GenServer.call(pid, {:discard, delivery_tag})
  end

  def requeue(pid, delivery_tag) do
    Logger.debug("Going to requeue #{inspect(delivery_tag)}")
    GenServer.call(pid, {:requeue, delivery_tag})
  end

  def start_message_tracker(sharding_key) do
    with {:ok, via_tuple} <- fetch_queue_via_tuple(sharding_key) do
      GenServer.call(via_tuple, {:start_message_tracker, sharding_key})
    end
  end

  def start_data_updater(sharding_key, message_tracker) do
    with {:ok, via_tuple} <- fetch_queue_via_tuple(sharding_key) do
      GenServer.call(via_tuple, {:start_data_updater, sharding_key, message_tracker})
    end
  end

  defp get_queue_via_tuple(queue_index) when is_integer(queue_index) do
    {:via, Registry, {Registry.AMQPDataConsumer, {:queue_index, queue_index}}}
  end

  defp fetch_queue_via_tuple(sharding_key) do
    GenServer.call(self(), {:fetch_queue_via_tuple, sharding_key})
  end

  # Server callbacks

  @impl true
  def init(args) do
    queue_name = Keyword.fetch!(args, :queue_name)
    queue_range_start = Keyword.fetch!(args, :range_start)
    queue_range_end = Keyword.fetch!(args, :range_end)
    queue_count = Keyword.fetch!(args, :queue_total_count)
    message_handler = Keyword.fetch!(args, :message_handler)

    state = %State{
      queue_name: queue_name,
      queue_range: queue_range_start..queue_range_end,
      queue_total_count: queue_count,
      message_handler: message_handler
    }

    {:ok, state, {:continue, :init_consume}}
  end

  @impl true
  def handle_continue(:init_consume, state), do: init_consume(state)

  @impl true
  def handle_call({:ack, delivery_tag}, _from, %State{channel: chan} = state) do
    res = @adapter.ack(chan, delivery_tag)
    {:reply, res, state}
  end

  def handle_call({:discard, delivery_tag}, _from, %State{channel: chan} = state) do
    res = @adapter.reject(chan, delivery_tag, requeue: false)
    {:reply, res, state}
  end

  def handle_call({:requeue, delivery_tag}, _from, %State{channel: chan} = state) do
    res = @adapter.reject(chan, delivery_tag, requeue: true)
    {:reply, res, state}
  end

  # TODO this was (seemed to be) unused
  def handle_call({:start_message_tracker, sharding_key}, _from, state) do
    res = DataUpdater.get_message_tracker(sharding_key)
    {:reply, res, state}
  end

  # TODO this was (seemed to be) unused
  def handle_call({:start_data_updater, sharding_key, message_tracker}, _from, state) do
    %State{message_handler: message_handler} = state
    res = DataUpdater.get_data_updater_process(sharding_key, message_tracker, message_handler)
    {:reply, res, state}
  end

  def handle_call({:fetch_queue_via_tuple, sharding_key}, _from, state) do
    %State{
      queue_total_count: queue_count,
      queue_range: queue_range
    } = state

    # TODO refactor: bring out the algorithm
    # This is the same sharding algorithm used in producer
    # Make sure they stay in sync
    queue_index = :erlang.phash2(sharding_key, queue_count)

    if queue_index in queue_range do
      {:reply, {:ok, get_queue_via_tuple(queue_index)}, state}
    else
      {:reply, {:error, :unhandled_device}, state}
    end
  end

  @impl true
  def handle_info(:init_consume, state), do: init_consume(state)

  def handle_info(
        {:DOWN, _, :process, pid, :normal},
        %State{channel: %Channel{pid: chan_pid}} = state
      )
      when pid != chan_pid do
    # This is a Message Tracker deactivating itself normally, do nothing
    {:noreply, state}
  end

  # Make sure to handle monitored message trackers exit messages
  # Under the hood DataUpdater calls Process.monitor so those monitor are leaked into this process.
  def handle_info(
        {:DOWN, monitor, :process, chan_pid, reason},
        %{monitor: monitor, channel: %{pid: chan_pid}} = state
      ) do
    # Channel went down, stop the process
    Logger.warning("AMQP data consumer crashed, reason: #{inspect(reason)}",
      tag: "data_consumer_chan_crash"
    )

    init_consume(%State{state | channel: nil, monitor: nil})
  end

  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Message consumed
  def handle_info({:basic_deliver, payload, meta}, state) do
    %State{channel: chan, message_handler: message_handler} = state
    {headers, no_headers_meta} = Map.pop(meta, :headers, [])
    headers_map = amqp_headers_to_map(headers)

    {timestamp, clean_meta} = Map.pop(no_headers_meta, :timestamp)

    case handle_consume(payload, headers_map, timestamp, clean_meta, message_handler) do
      :ok ->
        :ok

      :invalid_msg ->
        # ACK invalid msg to discard them
        @adapter.ack(chan, meta.delivery_tag)
    end

    {:noreply, state}
  end

  defp schedule_connect() do
    Process.send_after(self(), :init_consume, @reconnect_interval)
  end

  defp init_consume(state) do
    conn = ExRabbitPool.get_connection_worker(:events_consumer_pool)

    case ExRabbitPool.checkout_channel(conn) do
      {:ok, channel} ->
        try_to_setup_consume(channel, conn, state)

      {:error, reason} ->
        _ =
          Logger.warning(
            "Failed to check out channel for consumer on queue #{state.queue_name}: #{inspect(reason)}",
            tag: "channel_checkout_fail"
          )

        schedule_connect()
        {:noreply, state}
    end
  end

  defp try_to_setup_consume(channel, conn, state) do
    %Channel{pid: channel_pid} = channel
    %State{queue_name: queue_name} = state

    with :ok <- @adapter.qos(channel, prefetch_count: @consumer_prefetch_count),
         {:ok, _queue} <- @adapter.declare_queue(channel, queue_name, durable: true),
         {:ok, _consumer_tag} <- @adapter.consume(channel, queue_name, self()) do
      ref = Process.monitor(channel_pid)

      _ =
        Logger.debug("AMQPDataConsumer for queue #{queue_name} initialized",
          tag: "data_consumer_init_ok"
        )

      {:noreply, %State{state | channel: channel, monitor: ref}}
    else
      {:error, reason} ->
        Logger.warning(
          "Error initializing AMQPDataConsumer on queue #{state.queue_name}: #{inspect(reason)}",
          tag: "data_consumer_init_err"
        )

        # Something went wrong, let's put the channel back where it belongs
        _ = ExRabbitPool.checkin_channel(conn, channel)
        schedule_connect()
        {:noreply, %{state | channel: nil, monitor: nil}}
    end
  end

  defp handle_consume(payload, headers, timestamp, meta, message_handler) do
    with %{@sharding_key => sharding_key_binary} <- headers,
         {:ok, tracking_id} <- get_tracking_id(meta) do
      sharding_key = :erlang.binary_to_term(sharding_key_binary)
      # This call might spawn processes and implicitly monitor them
      DataUpdater.handle_message(
        sharding_key,
        payload,
        headers,
        tracking_id,
        timestamp,
        message_handler
      )
    else
      _ -> handle_invalid_msg(payload, headers, timestamp, meta)
    end
  end

  defp handle_invalid_msg(payload, headers, timestamp, meta) do
    Logger.warning(
      "Invalid AMQP message: #{inspect(Base.encode64(payload))} #{inspect(headers)} #{inspect(timestamp)} #{inspect(meta)}",
      tag: "data_consumer_invalid_msg"
    )

    :invalid_msg
  end

  defp amqp_headers_to_map(headers) do
    Enum.reduce(headers, %{}, fn {key, _type, value}, acc ->
      Map.put(acc, key, value)
    end)
  end

  defp get_tracking_id(meta) do
    message_id = meta.message_id
    delivery_tag = meta.delivery_tag

    if is_binary(message_id) and is_integer(delivery_tag) do
      {:ok, {meta.message_id, meta.delivery_tag}}
    else
      {:error, :invalid_message_metadata}
    end
  end
end
