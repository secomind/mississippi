defmodule Mississippi.Consumer.DataUpdater.Handler.Impl do
  @moduledoc false
  @behaviour Mississippi.Consumer.DataUpdater.Handler

  @impl true
  def init(sharding_key) do
    IO.puts("Handling data with sharding_key #{inspect(sharding_key)}")
    {:ok, sharding_key}
  end

  @impl true
  def handle_message(payload, headers, message_id, timestamp, state) do
    IO.puts(
      "Received message #{inspect(message_id)} with payload #{inspect(payload)} and headers #{inspect(headers)} at #{inspect(DateTime.from_unix!(timestamp))}"
    )

    {:ok, :ok, state}
  end

  @impl true
  def handle_signal(signal, state) do
    IO.puts("Received signal #{inspect(signal)}")

    {:ok, state}
  end

  @impl true
  def terminate(reason, state) do
    IO.puts("Terminating on #{inspect(reason)}, state: #{inspect(state)}")
    :ok
  end
end
