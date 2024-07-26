defmodule FakeConnectionAdapter do
  @moduledoc false
  use Agent

  alias FakeConnectionAdapter.State

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field(:parent_process, pid())
      field(:channel_queues, %{AMQP.Channel.t() => []}, default: %{})
    end
  end

  def start(parent_process) do
    Agent.start(fn -> %State{parent_process: parent_process} end, name: __MODULE__)
  end

  def declare_queue(channel, queue_name, opts \\ []) do
    queue = {queue_name, opts}

    Agent.update(__MODULE__, fn state ->
      updated_queues =
        Map.update(state.channel_queues, channel, [queue], &[queue | &1])

      send(state.parent_process, {:queue_declared, queue_name})

      %State{state | channel_queues: updated_queues}
    end)

    {:ok, %{}}
  end

  def publish(channel, _exchange, routing_key, payload, _opts \\ []) do
    state = Agent.get(__MODULE__, & &1)

    valid? =
      state.channel_queues
      |> Map.get(channel, [])
      |> Enum.find(fn {key, _} -> key == routing_key end)

    if valid? do
      send(state.parent_process, {:published, channel, routing_key, payload})
    end

    :ok
  end
end
