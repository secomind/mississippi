# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.AMQPDataConsumer.Starter do
  @moduledoc false
  use Task, restart: :transient

  alias Mississippi.Consumer.AMQPDataConsumer

  require Logger

  def start_link(args) do
    Task.start_link(__MODULE__, :run, [args])
  end

  def run(args) do
    AMQPDataConsumer.Supervisor.start_consumers(args)
  end
end
