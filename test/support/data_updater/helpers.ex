# Copyright 2026 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.DataUpdater.Helpers do
  @moduledoc """
  Helpers for DataUpdaters in tests
  """

  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.DataUpdater.Handler.Impl
  alias Mississippi.Consumer.MessageTracker

  require Hammox
  require Mimic

  def setup_data_updater!(sharding_key) do
    {:ok, data_updater} =
      DataUpdater.get_data_updater_process(sharding_key)

    do_setup_data_updater!(data_updater)

    data_updater
  end

  def setup_standalone_data_updater!(message_handler \\ Impl, sharding_key) do
    opts = [message_handler: message_handler, sharding_key: sharding_key]
    {:ok, data_updater} = GenServer.start_link(DataUpdater, opts)
    do_setup_data_updater!(data_updater)
    data_updater
  end

  defp do_setup_data_updater!(data_updater) do
    :erlang.trace(data_updater, true, [:receive])

    Mimic.allow(MessageTracker, self(), data_updater)
    Hammox.allow(MockMessageHandler, self(), data_updater)
  end
end
