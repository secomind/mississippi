defmodule Mississippi.Consumer.DataUpdater do
  @moduledoc """
  The DataUpdater process takes care of handling messages and signals for a given sharding key.
  Messages are handled using the `message_handler` providedin the Mississippi config,
  which is a module implementing DataUpdater.Handler behaviour.
  Note that the DataUpdater process has no concept of message ordering, as it is the
  MessageTracker process that takes care of maitaining the order of messages.
  """

  use GenServer, restart: :transient
  use Efx

  alias Horde.Registry
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.DataUpdater.State
  alias Mississippi.Consumer.Message
  alias Mississippi.Consumer.MessageTracker

  require Logger

  # TODO make this configurable?
  @data_updater_deactivation_interval_ms :timer.hours(3)

  @doc """
  Handle a message using the `message_handler` provided in the Mississippi config
  (which is a module implementing DataUpdater.Handler behaviour).
  You can get the DataUpdater instance for a given sharding_key using `get_data_updater_process/1`.
  """
  def handle_message(data_updater_pid, %Message{} = message) do
    GenServer.cast(data_updater_pid, {:handle_message, message})
  end

  @doc """
  Handles an information that must be forwarded to a `message_handler` but is not a Mississippi message.
  Used to change the state of a stateful Handler. The call is blocking and there is no ordering guarantee.
  You can get the DataUpdater instance for a given sharding_key using `get_data_updater_process/1`.
  """
  def handle_signal(data_updater_pid, signal) do
    GenServer.call(data_updater_pid, {:handle_signal, signal})
  end

  @doc """
  Provides a reference to the DataUpdater process that will handle the set of messages identified by
  the given sharding key.
  """
  @spec get_data_updater_process(sharding_key :: term()) ::
          {:ok, pid()} | {:error, :data_updater_start_fail}
  defeffect get_data_updater_process(sharding_key) do
    # TODO bring back :offload_start (?)
    case DataUpdater.Supervisor.start_child({DataUpdater, sharding_key: sharding_key}) do
      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, _info} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      other ->
        _ =
          Logger.warning(
            "Could not start DataUpdater process for sharding_key #{inspect(sharding_key)}: #{inspect(other)}"
          )

        {:error, :data_updater_start_fail}
    end
  end

  def start_link(start_args) do
    start_link({:message_handler, Mississippi.Consumer.DataUpdater.Handler.Impl}, start_args)
  end

  def start_link(extra_args, start_args) do
    {:message_handler, message_handler} = extra_args
    sharding_key = Keyword.fetch!(start_args, :sharding_key)

    init_args = [
      sharding_key: sharding_key,
      message_handler: message_handler
    ]

    name = {:via, Registry, {DataUpdater.Registry, {:sharding_key, sharding_key}}}
    GenServer.start_link(__MODULE__, init_args, name: name)
  end

  @impl true
  def init(init_arg) do
    sharding_key = Keyword.fetch!(init_arg, :sharding_key)
    message_handler = Keyword.fetch!(init_arg, :message_handler)

    with {:ok, handler_state} <- message_handler.init(sharding_key) do
      state = %State{
        sharding_key: sharding_key,
        message_handler: message_handler,
        handler_state: handler_state
      }

      {:ok, state, @data_updater_deactivation_interval_ms}
    end
  end

  @impl true
  def handle_call({:handle_signal, signal}, _from, state) do
    {return_value, new_handler_state} =
      state.message_handler.handle_signal(signal, state.handler_state)

    new_state = %State{state | handler_state: new_handler_state}

    {:reply, return_value, new_state, @data_updater_deactivation_interval_ms}
  end

  @impl true
  def handle_cast({:handle_message, %Message{} = message}, state) do
    %Message{payload: payload, headers: headers, timestamp: timestamp, meta: meta} = message

    case state.message_handler.handle_message(
           payload,
           headers,
           meta.message_id,
           timestamp,
           state.handler_state
         ) do
      {:ack, _, new_handler_state} ->
        ack_message!(message, state.sharding_key)
        new_state = %State{state | handler_state: new_handler_state}
        {:noreply, new_state, @data_updater_deactivation_interval_ms}

      {:ack, _, new_handler_state, {:continue, continue_arg}} ->
        ack_message!(message, state.sharding_key)
        new_state = %State{state | handler_state: new_handler_state}
        {:noreply, new_state, {:continue, continue_arg}}

      {:discard, reason, new_handler_state} ->
        reject_message!(message, state.sharding_key, reason)
        new_state = %State{state | handler_state: new_handler_state}
        {:noreply, new_state, @data_updater_deactivation_interval_ms}

      {:discard, reason, new_handler_state, {:continue, continue_arg}} ->
        reject_message!(message, state.sharding_key, reason)
        new_state = %State{state | handler_state: new_handler_state}
        {:noreply, new_state, {:continue, continue_arg}}

      {:stop, reason, action, new_handler_state} ->
        case action do
          :ack -> ack_message!(message, state.sharding_key)
          :discard -> reject_message!(message, state.sharding_key, reason)
        end

        new_state = %State{state | handler_state: new_handler_state}
        {:stop, {:shutdown, :requested}, new_state}
    end
  end

  @impl true
  def handle_continue(continue_arg, state) do
    %State{message_handler: message_handler, handler_state: handler_state} = state
    {:ok, new_handler_state} = message_handler.handle_continue(continue_arg, handler_state)
    new_state = %State{state | handler_state: new_handler_state}

    {:noreply, new_state, @data_updater_deactivation_interval_ms}
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
    {:stop, :normal, state}
  end

  @impl true
  def terminate(reason, state) do
    %State{message_handler: message_handler, handler_state: handler_state} = state
    message_handler.terminate(reason, handler_state)
    :ok
  end

  defp ack_message!(%Message{} = message, sharding_key) do
    _ = Logger.debug("Successfully handled message #{inspect(message.meta.message_id)}")
    {:ok, message_tracker} = MessageTracker.get_message_tracker(sharding_key)
    MessageTracker.ack_delivery(message_tracker, message)
  end

  defp reject_message!(%Message{} = message, sharding_key, reason) do
    _ =
      Logger.warning("Error handling message #{inspect(message.meta.message_id)}, reason #{inspect(reason)}")

    {:ok, message_tracker} = MessageTracker.get_message_tracker(sharding_key)
    MessageTracker.reject(message_tracker, message)
  end
end
