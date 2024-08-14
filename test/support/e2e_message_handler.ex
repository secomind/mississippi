defmodule E2EMessageHandler do
  @moduledoc false
  @behaviour Mississippi.Consumer.DataUpdater.Handler

  alias Mississippi.Consumer.DataUpdater.Handler

  @impl Handler
  def init(sharding_key) do
    {:ok, sharding_key}
  end

  @impl Handler
  def handle_message(payload, headers, _message_id, timestamp, state) do
    agent_state = Agent.get(__MODULE__, fn s -> s end)
    Process.send(agent_state.receiver, {payload, headers, timestamp}, [])
    {:ok, :ok, state}
  end

  @impl Handler
  def handle_signal(signal, state) do
    agent_state = Agent.get(__MODULE__, fn state -> state end)
    Process.send(agent_state.receiver, signal, [])
    {:ok, state}
  end

  @impl Handler
  def handle_continue(_continue_arg, state) do
    {:ok, state}
  end

  @impl Handler
  def terminate(_reason, _state) do
    :ok
  end

  def start_with_receiver(receiver) do
    Agent.start_link(fn -> %{receiver: receiver} end, name: __MODULE__)
  end
end
