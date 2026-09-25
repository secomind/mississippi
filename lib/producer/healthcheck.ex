# Copyright 2026 Clea Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.Healthcheck do
  @moduledoc """
  Healthcheck functions for the EventsProducer supervision tree.
  """

  alias Horde.DynamicSupervisor
  alias Mississippi.Producer.EventsProducer.Worker

  @doc """
  Returns true only if at least one worker is running and all of them
  have an active AMQP channel.
  """
  def all_amqp_channels_up? do
    children = DynamicSupervisor.which_children(Worker.Supervisor)

    all_channels_up? =
      Enum.all?(children, fn {_, pid, :worker, _} ->
        Worker.get_amqp_channel_status(pid) == :up
      end)

    # no active workers means the system is not healthy
    # TODO add a healthcheck on the number of running workers
    children != [] and all_channels_up?
  end
end
