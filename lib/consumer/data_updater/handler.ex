defmodule Mississippi.Consumer.DataUpdater.Handler do
  @moduledoc """
  A behaviour module to for implementing a Processor of Mississippi messages.
  Messages sharing the same sharding key are processed in order.
  """

  @doc """
  Invoked when a message is received. A return value of `{:ok, result}` will make Mississippi ack the message,
  while `{:error, reason}` will make Mississippi discard it.
  """
  @callback handle_message(
              payload :: term(),
              headers :: map(),
              message_id :: binary(),
              timestamp :: term()
            ) ::
              {:ok, result :: term()} | {:error, reason :: term()}
end
