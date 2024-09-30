# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.MessageTracker do
  @moduledoc """
  The MessageTracker process guarantees that messages sharing the same sharding key
  are processed in (chronological) order.
  Under the hood, messages are put in a FIFO queue, and the next message is processed
  only if the current one has been handled (either acked or rejected).
  In order to maintain the strong ordering guarantee, it is possible that in some corner cases
  a message gets processed twice, but after all with strange aeons, even death may die.
  """

  use Efx

  alias Horde.DynamicSupervisor
  alias Horde.Registry
  alias Mississippi.Consumer.Message
  alias Mississippi.Consumer.MessageTracker

  require Logger

  @doc """
  Provides a reference to the MessageTracker process that will track the set of messages identified by
  the given sharding key.
  """
  @spec get_message_tracker(sharding_key :: term()) ::
          {:ok, pid()} | {:error, :message_tracker_start_fail}
  defeffect get_message_tracker(sharding_key) do
    name = {:via, Registry, {MessageTracker.Registry, {:sharding_key, sharding_key}}}

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
            "Could not start MessageTracker process for sharding_key #{inspect(sharding_key)}: #{inspect(other)}"
          )

        {:error, :message_tracker_start_fail}
    end
  end

  @doc """
  Starts handling a message. This call is not blocking.
  The message will be put in the MessageTracker process FIFO queue, and processed
  when it is on top of it.
  """
  def handle_message(message_tracker, %Message{} = message, channel) do
    GenServer.cast(message_tracker, {:handle_message, message, channel})
  end

  @doc """
  Signals to the MessageTracker process to ack the message.
  This call is blocking, as only first in-order message can be acked.
  """
  def ack_delivery(message_tracker, %Message{} = message) do
    GenServer.call(message_tracker, {:ack_delivery, message})
  end

  @doc """
  Signals to the MessageTracker process to reject the message.
  This call is blocking, as only first in-order message can be rejected.
  """
  def reject(message_tracker, %Message{} = message) do
    GenServer.call(message_tracker, {:reject, message})
  end
end
