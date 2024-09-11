# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.AMQPDataConsumer.State do
  @moduledoc false
  use TypedStruct

  typedstruct do
    field :queue_name, String.t(), enforce: true
    field :monitors, list(), enforce: true
    field :channel, term()
    field :connection, module()
  end
end
