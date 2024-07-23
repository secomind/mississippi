defmodule Mississippi.Consumer.AMQPDataConsumer.AMQPConnection do
  @moduledoc """
  A behaviour module to for implementing a DataConsumer AMQP connection.
  """

  alias AMQP.Channel
  alias Mississippi.Consumer.AMQPDataConsumer.State

  @doc """
  The adapter module for the AMQP connection
  """
  @callback adapter() :: module()

  @doc """
  Initializes the AMQP connection.
  Returns `{:ok, channel}` if it was initialized successfully, or `{:error, reason}` if it was not.
  """
  @callback init(state :: State.t()) :: {:ok, Channel.t()} | {:error, reason :: term()}
end
