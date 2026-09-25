# Copyright 2026 Clea Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.Healthcheck.Test do
  use ExUnit.Case, async: false

  import Hammox

  alias AMQP.Channel
  alias AMQP.Connection
  alias Horde.Registry
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.State
  alias Mississippi.Producer.EventsProducer.Worker
  alias Mississippi.Producer.Healthcheck
  alias Mississippi.Producer.ProducersSupervisor

  @moduletag :unit

  setup do
    Hammox.set_mox_global()
    test_process = self()

    start_supervised!({FakeConnectionAdapter, test_process})

    MockAMQPConnection
    |> stub(:init, fn _ -> {:ok, channel_fixture()} end)
    |> stub(:adapter, fn -> FakeConnectionAdapter end)

    total_count = 2

    start_supervised!(events_producer_supervisor_fixture(total_count))

    producers = event_producer_pids(total_count)

    for {_id, pid} <- producers, do: :erlang.trace(pid, true, [:receive])

    %{producers: producers}
  end

  describe "check_all" do
    test "returns ok when all the workers are running and all the AMQP channels are up" do
      # setup ensures the workers and the AMQP channels are seen as running and functional
      assert Healthcheck.check_all() == :ok
    end

    test "returns error when no workers are running", %{producers: producers} do
      # kill all workers and have them not restarted by supervisor
      for {_, producer_pid} <- producers do
        DynamicSupervisor.terminate_child(Worker.Supervisor, producer_pid)
      end

      assert Healthcheck.check_all() == {:error, [:workers_count_mismatch, :no_active_workers]}
    end

    test "returns error when the AMQP channel process of at least one worker is not alive", %{
      producers: producers
    } do
      [{0, producer_pid_0} | _] = producers
      %State{channel: %{pid: channel_pid_0}} = :sys.get_state(producer_pid_0)

      # forcing the AMQP channel to be seen as 'down' when the producer is restarted
      stub(MockAMQPConnection, :init, fn _ -> {:error, :econnrefused} end)

      Process.exit(channel_pid_0, :kill)

      retry(10, "Wait for worker to crash", fn ->
        not Process.alive?(producer_pid_0)
      end)

      assert Healthcheck.check_all() == {:error, [:amqp_channels_down]}
    end

    test "returns error when not all the expected workers are running", %{producers: producers} do
      [{0, producer_pid_0} | _] = producers

      # kill (only) one worker and have it not restarted by supervisor
      :ok = DynamicSupervisor.terminate_child(Worker.Supervisor, producer_pid_0)

      assert Healthcheck.check_all() == {:error, [:workers_count_mismatch]}
    end

    test "returns error when the producer config is not loaded in the Registry" do
      Registry.unregister(EventsProducer.Registry, :events_producer_config)

      retry(10, "Wait for configuration deletion", fn ->
        Registry.lookup(EventsProducer.Registry, :events_producer_config) == []
      end)

      assert Healthcheck.check_all() == {:error, [:events_producer_uninitialized]}
    end
  end

  defp temporary_process do
    spawn(fn ->
      receive do
        _ -> nil
      end
    end)
  end

  defp channel_fixture(channel_pid \\ nil, connection_pid \\ nil) do
    channel_pid = channel_pid || temporary_process()
    connection_pid = connection_pid || self()

    %Channel{
      pid: channel_pid,
      conn: %Connection{pid: connection_pid}
    }
  end

  defp events_producer_supervisor_fixture(total_count) do
    opts = [
      queues: [
        total_count: total_count,
        ssl_options: [verify: :verify_none],
        events_exchange_name: "",
        connection: MockAMQPConnection,
        reconnection_backoff_ms: 0
      ]
    ]

    {ProducersSupervisor, opts}
  end

  defp event_producer_pids(total_count) do
    last_index = total_count - 1

    0..last_index
    |> Enum.map(&{&1, event_producer_pid(&1)})
  end

  def event_producer_pid(queue_index) do
    Worker.via_tuple(queue_index)
    |> GenServer.whereis()
  end

  defp retry(count, message, check_fun) do
    case {count, check_fun.()} do
      {_, true} ->
        :ok

      {0, false} ->
        flunk(message)

      {n, false} ->
        Process.sleep(10)
        retry(n - 1, message, check_fun)
    end
  end
end
