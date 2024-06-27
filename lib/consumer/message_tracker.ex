defmodule Mississippi.Consumer.MessageTracker do
  @moduledoc """
  The MessageTracker process guarantees that messages sharing the same sharding key
  are processed in (chronological) order.
  """

  @doc """
  Start tracking a message. This call is not blocking.
  """
  def track_delivery(message_tracker, message_id, delivery_tag) do
    GenServer.cast(message_tracker, {:track_delivery, message_id, delivery_tag})
  end

  @doc """
  Allows the MessageTracker to signal to the AMQPConsumer process to ack the message identified by `message_id`.
  This call is blocking, as only first in-order message can be acked.
  """
  def ack_delivery(message_tracker, message_id) do
    GenServer.call(message_tracker, {:ack_delivery, message_id})
  end

  @doc """
  Allows the MessageTracker to signal to the AMQPConsumer process to discard the message identified by `message_id`.
  This call is blocking, as only first in-order message can be discarded.
  """
  def discard(message_tracker, message_id) do
    GenServer.call(message_tracker, {:discard, message_id})
  end

  @doc """
  Invoked to deactivate a given MessageTracker process.
  """
  def deactivate(message_tracker) do
    GenServer.call(message_tracker, :deactivate, :infinity)
  end
end
