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
  Invoked when a message is received. A return value of `{:ok, result, state}` will make Mississippi ack the message,
  while `{:error, reason, state}` will make Mississippi discard it.
  """
  @callback handle_message(
              payload :: term(),
              headers :: map(),
              message_id :: binary(),
              timestamp :: term(),
              state :: handler_state
            ) ::
              {:ok, result :: term(), state :: handler_state} | {:error, reason :: term()}

  @doc """
  Invoked when the handler is being terminated. It can be used to perform cleanup tasks before closing.
  """
  @callback terminate(reason :: term(), state :: handler_state) :: :ok
end
