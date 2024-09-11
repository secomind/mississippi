# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.MessageTracker.Server.State do
  @moduledoc false
  use TypedStruct

  typedstruct do
    field :queue, term(), enforce: true

    field :sharding_key, term(), enforce: true

    field :data_updater_pid, pid()

    field :channel, AMQP.Channel.t()
  end
end
