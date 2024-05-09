defmodule Mississippi.Consumer.DataUpdater.Handler.Impl do
  @behaviour Mississippi.Consumer.DataUpdater.Handler

  @impl true
  def handle_message(payload, headers, message_id, timestamp) do
    IO.puts(
      "Received message #{inspect(message_id)} with payload #{inspect(payload)} and headers #{inspect(headers)}
      at #{inspect(timestamp |> DateTime.from_unix!())}"
    )

    {:ok, :ok}
  end
end
