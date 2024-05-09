defmodule Mississippi.Consumer.DataUpdater do
  defmodule State do
    defstruct [
      :sharding_key,
      :message_tracker,
      :message_handler
    ]
  end

  use GenServer

  alias Mississippi.Consumer.AMQPDataConsumer
  alias Mississippi.Consumer.MessageTracker
  require Logger

  # TODO make this configurable?
  @data_updater_deactivation_interval_ms 10_000

  @doc """
  Start handling a message. If it is the first in-order message, it will be processed
  straight away by the `message_handler` (which is a module implementing DataUpdater.Handler behaviour).
  If not, the message will remain in memory until it can be processed, i.e. it is now the first
  in-order message.
  """
  def handle_message(sharding_key, payload, headers, tracking_id, timestamp, message_handler) do
    message_tracker = get_message_tracker(sharding_key)
    {message_id, delivery_tag} = tracking_id
    MessageTracker.track_delivery(message_tracker, message_id, delivery_tag)

    get_data_updater_process(sharding_key, message_tracker, message_handler)
    |> GenServer.cast({:handle_message, payload, headers, message_id, timestamp})
  end

  @doc """
  Provides a reference to the DataUpdater process that will handle the set of messages identified by
  the given sharding key. Messages going through the DataUpdater process will be tracked by the
  `message_tracker` and the `message_handler` will be used to process them.
  """
  def get_data_updater_process(sharding_key, message_tracker, message_handler, opts \\ []) do
    case Registry.lookup(Registry.DataUpdater, sharding_key) do
      [] ->
        if Keyword.get(opts, :offload_start) do
          # We pass through AMQPDataConsumer to start the process to make sure that
          # that start is serialized
          AMQPDataConsumer.start_data_updater(sharding_key, message_tracker)
        else
          name = {:via, Registry, {Registry.DataUpdater, sharding_key}}
          {:ok, pid} = start(sharding_key, message_tracker, message_handler, name: name)
          pid
        end

      [{pid, nil}] ->
        pid
    end
  end

  @doc """
  Provides a reference to the MessageTracker process that will track the set of messages identified by
  the given sharding key. The MessageTracker process is linked to the one calling this function.
  """
  def get_message_tracker(sharding_key, opts \\ []) do
    case Registry.lookup(Registry.MessageTracker, sharding_key) do
      [] ->
        if Keyword.get(opts, :offload_start) do
          # We pass through AMQPDataConsumer to start the process to make sure that
          # that start is serialized and acknowledger is the right process
          AMQPDataConsumer.start_message_tracker(sharding_key)
        else
          acknowledger = self()
          spawn_message_tracker(acknowledger, sharding_key)
        end

      [{pid, nil}] ->
        pid
    end
  end

  @doc """
  Starts a DataUpdater process that will handle the set of messages identified by
  `sharding_key`. Messages going through the DataUpdater process will be tracked by the
  `message_tracker` and the `message_handler` will be used to process them.
  """
  def start(sharding_key, message_tracker, message_handler, opts \\ []) do
    init_arg = [
      sharding_key: sharding_key,
      message_tracker: message_tracker,
      message_handler: message_handler
    ]

    GenServer.start(__MODULE__, init_arg, opts)
  end

  @impl true
  def init(init_arg) do
    sharding_key = Keyword.fetch!(init_arg, :sharding_key)
    message_tracker = Keyword.fetch!(init_arg, :message_tracker)
    message_handler = Keyword.fetch!(init_arg, :message_handler)

    MessageTracker.register_data_updater(message_tracker)
    Process.monitor(message_tracker)

    state = %State{
      sharding_key: sharding_key,
      message_tracker: message_tracker,
      message_handler: message_handler
    }

    {:ok, state, @data_updater_deactivation_interval_ms}
  end

  @impl true
  def handle_cast({:handle_message, payload, headers, message_id, timestamp}, state) do
    if MessageTracker.can_process_message(state.message_tracker, message_id) do
      case state.message_handler.handle_message(payload, headers, message_id, timestamp) do
        {:ok, _} ->
          _ = Logger.debug("Successfully handled message #{inspect(message_id)}")
          MessageTracker.ack_delivery(state.message_tracker, message_id)

        {:error, reason} ->
          _ =
            Logger.warning(
              "Error handling message #{inspect(message_id)}, reason #{inspect(reason)}"
            )

          MessageTracker.discard(state.message_tracker, message_id)
      end
    end

    {:noreply, state, @data_updater_deactivation_interval_ms}
  end

  @impl true
  def handle_info({:DOWN, _, :process, pid, :normal}, %{message_tracker: pid} = state) do
    # This is a MessageTracker normally terminating due to deactivation
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _, :process, _pid, :shutdown}, state) do
    {:stop, :shutdown, state}
  end

  @impl true
  def handle_info({:DOWN, _, :process, _pid, _reason}, state) do
    {:stop, :monitored_process_died, state}
  end

  @impl true
  def handle_info(:timeout, state) do
    :ok = MessageTracker.deactivate(state.message_tracker)

    {:stop, :normal, state}
  end

  defp spawn_message_tracker(acknowledger, sharding_key) do
    name = {:via, Registry, {Registry.MessageTracker, sharding_key}}
    {:ok, pid} = MessageTracker.start_link(acknowledger: acknowledger, name: name)

    pid
  end
end
