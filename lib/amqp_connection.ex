# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.AMQPConnection do
  @moduledoc """
  A behaviour module to for implementing a DataConsumer AMQP connection.
  """

  alias AMQP.Channel

  @doc """
  The adapter module for the AMQP connection
  """
  @callback adapter() :: module()

  @doc """
  Initializes the AMQP connection.
  Returns `{:ok, channel}` if it was initialized successfully, or `{:error, reason}` if it was not.
  """
  @callback init(state :: term()) :: {:ok, Channel.t()} | {:error, reason :: term()}
end
