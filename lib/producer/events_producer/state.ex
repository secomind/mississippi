# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.State do
  @moduledoc false
  use TypedStruct

  typedstruct do
    field :events_exchange_name, String.t(), enforce: true
    field :queue_prefix, String.t(), enforce: true
    field :queue_total_count, pos_integer(), enforce: true
    field :channel, term()
    field :connection, module()
    field :reconnection_backoff_ms, non_neg_integer()
  end
end
