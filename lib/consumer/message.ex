# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.Message do
  @moduledoc false
  use TypedStruct

  typedstruct do
    field :payload, term(), enforce: true
    field :headers, map(), enforce: true
    field :timestamp, term(), enforce: true
    field :meta, term(), enforce: true
  end
end
