# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.DataUpdater.Handler do
  @moduledoc """
  A behaviour module to for implementing a Processor of Mississippi messages.
  Messages sharing the same sharding key are processed in order.
  """

  @type handler_state :: term()

  @doc """
  Invoked when the handler is initialized. This can be used to make the handler stateful.
  The sharding key for this Handler  is passed as an argument to the callback.
  An `{:error, reason}` return value will make handler initialization fail. In that case, the
  handler will be restarted and messages which have been prefetched will be requed in the original order.
  """
  @callback init(sharding_key :: term()) ::
              {:ok, state :: handler_state} | {:error, reason :: term()}

  @doc """
  Invoked when a message is received. Possible return values are:
  * `{:ack, result, state}`: Mississippi acks the message
  * `{:ack, result, state, {:continue, continue_arg}}`: Mississippi acks the message and
    the DataUpdater process continues as specified by the `handle_continue/2` callback
  * `{:discard, reason, state}`: Mississippi discards the message
  * `{:discard, result, state, {:continue, continue_arg}}`: Mississippi discards the message and
    the DataUpdater process continues as specified by the `handle_continue/2` callback
  * `{:stop, reason, action, state}`: Mississippi acks or discards the message according to `action`
    and then the DataUpdater process terminates normally.
    This makes the related MessageTracker terminate, too.
  """
  @callback handle_message(
              payload :: term(),
              headers :: map(),
              message_id :: binary(),
              timestamp :: term(),
              state :: handler_state
            ) ::
              {:ack, result :: term(), new_state :: handler_state}
              | {:ack, result :: term(), new_state :: handler_state, {:continue, continue_arg :: term()}}
              | {:discard, reason :: term(), new_state :: handler_state}
              | {:discard, reason :: term(), new_state :: handler_state, {:continue, continue_arg :: term()}}
              | {:stop, reason :: term(), action :: :ack | :discard, new_state :: handler_state}

  @doc """
  Invoked when an information that is not a message is received.
  Used to update the state of a stateful handler.
  There is no guarantee of ordering.
  """
  @callback handle_signal(signal :: term(), state :: handler_state) ::
              {result :: term(), new_state :: handler_state}

  @doc """
  Invoked when `handle_message/5` returns a `{:continue, continue_arg}` tuple.
  Used to update the state of a stateful handler.
  """
  @callback handle_continue(continue_arg :: term(), state :: handler_state) ::
              {:ok, new_state :: handler_state}

  @doc """
  Invoked when the handler is being terminated. It can be used to perform cleanup tasks before closing.
  """
  @callback terminate(reason :: term(), state :: handler_state) :: :ok
end
