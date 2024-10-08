# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.DataUpdater.State do
  @moduledoc false
  use TypedStruct

  typedstruct do
    field :sharding_key, term(), enforce: true
    field :message_handler, module(), enforce: true
    field :handler_state, term(), enforce: true
  end
end
