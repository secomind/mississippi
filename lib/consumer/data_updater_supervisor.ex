# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.DataUpdater.Supervisor do
  @moduledoc false
  use Horde.DynamicSupervisor

  alias Horde.DynamicSupervisor

  require Logger

  def start_link(init_args) do
    DynamicSupervisor.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  @impl true
  def init(init_args) do
    _ = Logger.info("Starting DataUpdater supervisor")
    DynamicSupervisor.init(strategy: :one_for_one, members: :auto, extra_arguments: init_args)
  end

  def start_child(child) do
    DynamicSupervisor.start_child(__MODULE__, child)
  end

  def terminate_child(pid) do
    _ =
      Logger.info("Terminating a DataUpdater")

    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end
end
