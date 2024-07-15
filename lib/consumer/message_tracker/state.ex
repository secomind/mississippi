defmodule Mississippi.Consumer.MessageTracker.Server.State do
  use TypedStruct

  typedstruct do
    field :queue, term(), enforce: true

    field :sharding_key, term(), enforce: true

    field :data_updater_pid, pid()

    field :channel, AMQP.Channel.t()
  end
end
