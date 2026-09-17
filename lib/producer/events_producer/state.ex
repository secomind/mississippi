# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.State do
  @moduledoc false
  use TypedStruct

  typedstruct do
    field :queue_name, String.t(), enforce: true
    field :queue_index, non_neg_integer(), enforce: true
    field :events_exchange_name, String.t(), enforce: true
    field :channel, term()
    field :connection, module()
    field :reconnection_backoff_ms, non_neg_integer(), default: 1_000
  end
end
