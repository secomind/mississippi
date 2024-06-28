defmodule Mississippi.Consumer.MessageTracker do
  @moduledoc """
  The MessageTracker process guarantees that messages sharing the same sharding key
  are processed in (chronological) order.
  """

  alias Mississippi.Consumer.MessageTracker
  alias Mississippi.Consumer.Message
  require Logger

  @doc """
  Provides a reference to the MessageTracker process that will track the set of messages identified by
  the given sharding key.
  """
  def get_message_tracker(sharding_key) do
    name = {:via, Registry, {Registry.MessageTracker, {:sharding_key, sharding_key}}}

    # TODO bring back :offload_start (?)
    case DynamicSupervisor.start_child(
           MessageTracker.Supervisor,
           {MessageTracker.Server, name: name, sharding_key: sharding_key}
         ) do
      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, _info} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      other ->
        _ =
          Logger.warning(
            "Could not start MessageTracker process for sharding_key #{inspect(sharding_key)}: #{inspect(other)}",
            tag: "message_tracker_start_fail"
          )

        {:error, :message_tracker_start_fail}
    end
  end

  @doc """
  Start tracking a message. This call is not blocking.
  """
  def handle_message(message_tracker, %Message{} = message, channel) do
    GenServer.cast(message_tracker, {:handle_message, message, channel})
  end

  @doc """
  Allows the MessageTracker to signal to the AMQPConsumer process to ack the message.
  This call is blocking, as only first in-order message can be acked.
  """
  def ack_delivery(message_tracker, %Message{} = message) do
    GenServer.call(message_tracker, {:ack_delivery, message})
  end

  @doc """
  Allows the MessageTracker to signal to the AMQPConsumer process to discard the message.
  This call is blocking, as only first in-order message can be discarded.
  """
  def discard(message_tracker, %Message{} = message) do
    GenServer.call(message_tracker, {:discard, message})
  end

  @doc """
  Invoked to deactivate a given MessageTracker process.
  """
  def deactivate(message_tracker) do
    GenServer.call(message_tracker, :deactivate, :infinity)
  end
end
