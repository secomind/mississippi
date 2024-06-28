defmodule Mississippi.Consumer.Message do
  defstruct [
    :payload,
    :headers,
    :timestamp,
    :meta
  ]
end
