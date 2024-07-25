defmodule Mississippi.Consumer.Test.Placeholder do
  @moduledoc false
  use GenServer

  @impl true
  def init(init_arg) do
    {:ok, init_arg}
  end

  @impl true
  def handle_call(_request, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(:die, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_cast(_, state) do
    {:noreply, state}
  end
end
