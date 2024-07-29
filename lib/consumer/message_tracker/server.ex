defmodule Mississippi.Consumer.MessageTracker.Server do
  @moduledoc """
  This module implements the MessageTracker process logic.
  """

  use GenServer, restart: :transient

  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.Message
  alias Mississippi.Consumer.MessageTracker.Server.State

  require Logger

  @adapter ExRabbitPool.RabbitMQ
  # TODO make this configurable? (Same as DataUpdater)
  @message_tracker_deactivation_interval_ms :timer.hours(3)

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    _ = Logger.debug("Starting MessageTracker #{inspect(name)}")
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(init_args) do
    sharding_key = Keyword.fetch!(init_args, :sharding_key)

    state = %State{
      queue: :queue.new(),
      sharding_key: sharding_key,
      data_updater_pid: nil,
      channel: nil
    }

    {:ok, state, @message_tracker_deactivation_interval_ms}
  end

  @impl true
  def handle_cast({:handle_message, %Message{} = message, new_channel}, state) do
    %State{channel: old_channel} = state

    new_state =
      if old_channel == nil or new_channel != old_channel do
        # If the channel is different, it means the old channel crashed. Update the reference.
        Process.monitor(new_channel.pid)
        %State{state | channel: new_channel}
      else
        state
      end

    {:noreply, new_state, {:continue, {:message_received, message}}}
  end

  @impl true
  def handle_call({:ack_delivery, %Message{} = message}, _from, state) do
    # Invariant: we're always processing the first message in the queue
    # Let us make sure
    %State{queue: queue} = state

    case :queue.peek(queue) do
      {:value, ^message} ->
        @adapter.ack(state.channel, message.meta.delivery_tag)
        new_state = %State{state | queue: :queue.drop(queue)}
        # let's move on to the next message
        {:reply, :ok, new_state, {:continue, :process_next_message}}

      _ ->
        # discard the message, we don't care for it anymore
        {:reply, :ok, state, @message_tracker_deactivation_interval_ms}
    end
  end

  @impl true
  def handle_call({:reject, %Message{} = message}, _from, state) do
    # Invariant: we're always processing the first message in the queue
    # Let us make sure
    %State{queue: queue} = state

    case :queue.peek(queue) do
      {:value, ^message} ->
        @adapter.reject(state.channel, delivery_tag_from_message(message))
        new_state = %State{state | queue: :queue.drop(queue)}
        # let's move on to the next message
        {:reply, :ok, new_state, {:continue, :process_next_message}}

      _ ->
        # discard the message, we don't care for it anymore
        {:reply, :ok, state, @message_tracker_deactivation_interval_ms}
    end
  end

  # If the channel has crashed, we should crash, too.
  # This way, no messages are acked on the wrong channel
  @impl true
  def handle_info({:DOWN, _ref, :process, down_pid, _reason}, %State{channel: channel} = state)
      when channel.pid == down_pid do
    {:stop, :channel_crashed, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, down_pid, _reason}, state) do
    %State{data_updater_pid: data_updater_pid} = state

    # Now, let's check if the DataUpdater crashed
    new_dup_pid = if down_pid == data_updater_pid, do: nil, else: data_updater_pid

    new_state = %State{state | data_updater_pid: new_dup_pid}

    {:noreply, new_state, {:continue, :process_next_message}}
  end

  @impl true
  def handle_info(:timeout, state) do
    {:stop, :normal, state}
  end

  @impl true
  def terminate(reason, _state) do
    _ =
      Logger.info("MessageTracker terminating with reason: #{inspect(reason)}")

    :ok
  end

  @impl true
  def handle_continue({:message_received, message}, state) do
    %State{queue: queue} = state

    new_queue = :queue.in(message, queue)
    new_state = %State{state | queue: new_queue}
    # :queue.len/1 runs in O(n), :queue.is_empty in O(1)
    if :queue.is_empty(state.queue) do
      {:noreply, new_state, {:continue, :process_next_message}}
    else
      {:noreply, new_state, @message_tracker_deactivation_interval_ms}
    end
  end

  @impl true
  def handle_continue(:process_next_message, state) do
    # We check if there are messages to handle
    if :queue.is_empty(state.queue) do
      # If not, we're ok
      {:noreply, state, @message_tracker_deactivation_interval_ms}
    else
      # otherwise, let's pick the next one...
      %{queue: queue, sharding_key: sharding_key} = state
      {:value, message} = :queue.peek(queue)
      # ... and tell the DU process to handle it
      {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)
      new_state = maybe_update_and_monitor_dup_pid(state, data_updater_pid)
      DataUpdater.handle_message(data_updater_pid, message)
      {:noreply, new_state, @message_tracker_deactivation_interval_ms}
    end
  end

  defp delivery_tag_from_message(%Message{} = message) do
    message.meta.delivery_tag
  end

  defp maybe_update_and_monitor_dup_pid(state, new_dup_pid) do
    %State{data_updater_pid: old_data_updater_pid} = state

    if old_data_updater_pid == new_dup_pid do
      state
    else
      Process.monitor(new_dup_pid)
      %State{state | data_updater_pid: new_dup_pid}
    end
  end
end
