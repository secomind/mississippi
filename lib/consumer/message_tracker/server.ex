defmodule Mississippi.Consumer.MessageTracker.Server do
  defmodule QueueEntry do
    defstruct [
      :channel,
      :data_consumer_pid,
      :message,
      :data_updater_pid
    ]
  end

  use GenServer
  require Logger
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.Message

  @adapter ExRabbitPool.RabbitMQ

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    _ = Logger.info("Starting MessageTracker #{inspect(name)}", tag: "message_tracker_start")
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(init_args) do
    sharding_key = Keyword.fetch!(init_args, :sharding_key)
    state = %{queue: :queue.new(), sharding_key: sharding_key}
    {:ok, state}
  end

  @impl true
  def handle_cast({:handle_message, %Message{} = message, channel}, state) do
    # :queue.len/1 runs in O(n)
    if :queue.is_empty(state.queue) do
      new_state = put_message_in_queue(message, channel, state)
      {:noreply, new_state, {:continue, :process_message}}
    else
      new_state = put_message_in_queue(message, channel, state)
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_call({:ack_delivery, %Message{} = message}, {dup_pid, _from}, state) do
    # Invariant: we're always processing the first message in the queue
    # Let us make sure?
    case :queue.peek(state.queue) do
      {:value, %QueueEntry{message: ^message, data_updater_pid: ^dup_pid} = entry} ->
        %QueueEntry{channel: channel} = entry

        # TODO check what's missing here! It seems that neither meta.delivery_tag nor meta.message_id are ok! Maybe it's channel and not channel_pid?
        @adapter.ack(channel, message.meta.delivery_tag)
        new_state = remove_head_from_queue(state)
        # let's move on to the next message
        {:reply, :ok, new_state, {:continue, :process_message}}

      _ ->
        # discard the message
        {:reply, :ok, state, {:continue, :process_message}}
    end
  end

  @impl true
  def handle_call({:discard, %Message{} = message}, {dup_pid, _from}, state) do
    # Invariant: we're always processing the first message in the queue
    # Let us make sure?
    case :queue.peek(state.queue) do
      {:value, %QueueEntry{message: ^message, data_updater_pid: ^dup_pid} = entry} ->
        %QueueEntry{channel: channel} = entry
        AMQP.Basic.nack(channel, delivery_tag_from_message(message))
        new_state = remove_head_from_queue(state)
        # let's move on to the next message
        {:reply, :ok, new_state, {:continue, :process_message}}

      _ ->
        # discard the message
        {:reply, :ok, state, {:continue, :process_message}}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, down_pid, _reason}, state) do
    # first of all, let's remove messages from crashed channels/data consumers
    %{queue: queue} = state

    active_messages =
      :queue.filter(
        fn %QueueEntry{} = entry ->
          entry.channel.pid != down_pid and entry.data_consumer_pid != down_pid
        end,
        queue
      )

    # Then, let's requeue messages from crashed DataUpdaters
    requeued_messages =
      :queue.filtermap(
        fn %QueueEntry{} = entry ->
          if entry.data_updater_pid == down_pid, do: %{entry | data_updater_pid: nil}, else: entry
        end,
        active_messages
      )

    # finally, continue to :process_message
    new_state = %{state | queue: requeued_messages}

    {:noreply, new_state, {:continue, :process_message}}
  end

  @impl true
  def handle_continue(:process_message, state) do
    # We check if there are messages to handle
    if :queue.is_empty(state.queue) do
      # If not, we're ok
      {:noreply, state}
    else
      # otherwise, let's pick the next one...
      %{sharding_key: sharding_key, queue: queue} = state
      {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)
      Process.monitor(data_updater_pid)
      # We spin to put the DUP pid in the entry
      {{:value, entry}, new_queue} = :queue.out(queue)
      %{message: %Message{} = message} = entry
      new_entry = %QueueEntry{entry | data_updater_pid: data_updater_pid}
      final_queue = :queue.in(new_entry, new_queue)

      # ... and tell  DUP to handle it
      # TODO make this PID-aware (i.e. DataUpdater.handle_message(pid, sharding_key, ....))
      DataUpdater.handle_message(data_updater_pid, message)
      {:noreply, %{state | queue: final_queue}}
    end
  end

  defp delivery_tag_from_message(%Message{} = message) do
    message.meta.delivery_tag
  end

  defp put_message_in_queue(message, channel, state) do
    %{queue: queue} = state

    entry = %QueueEntry{
      channel: channel,
      message: message
    }

    new_queue = :queue.in(entry, queue)
    %{state | queue: new_queue}
  end

  defp remove_head_from_queue(state) do
    %{queue: queue} = state

    new_queue = :queue.drop(queue)

    %{state | queue: new_queue}
  end
end
