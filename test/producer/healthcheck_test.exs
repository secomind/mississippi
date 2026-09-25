# Copyright 2026 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.Healthcheck.Test do
  use ExUnit.Case, async: false

  import Hammox

  alias AMQP.Channel
  alias AMQP.Connection
  alias Horde.DynamicSupervisor
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.State
  alias Mississippi.Producer.EventsProducer.Worker
  alias Mississippi.Producer.Healthcheck

  @moduletag :unit

  @reconnection_backoff_ms 60_000

  setup do
    Hammox.set_mox_global()

    start_supervised!({FakeConnectionAdapter, self()})
    start_worker_supervisors!()

    stub(MockAMQPConnection, :adapter, fn -> FakeConnectionAdapter end)

    :ok
  end

  describe "Healthcheck.all_amqp_channels_up?/0" do
    test "returns false when no workers are running" do
      refute Healthcheck.all_amqp_channels_up?()
    end

    test "returns true when every worker has a live AMQP channel" do
      stub_successful_init()

      Enum.each([0, 1], fn queue_index ->
        start_worker!(queue_index)
        assert_worker_connected(queue_index)
      end)

      assert Healthcheck.all_amqp_channels_up?()
    end

    test "returns false when at least one worker's AMQP channel is down" do
      test_process = self()

      stub(MockAMQPConnection, :init, fn
        %State{queue_index: 0} ->
          {:ok, channel_fixture()}

        %State{queue_index: queue_index} ->
          send(test_process, {:channel_init_failed, queue_index})
          {:error, :channel_init_failed}
      end)

      start_worker!(0)
      assert_worker_connected(0)

      worker = start_worker!(1)
      assert_worker_disconnected(1)
      assert Worker.get_amqp_channel_status(worker) == :down

      refute Healthcheck.all_amqp_channels_up?()
    end
  end

  describe "Worker.get_amqp_channel_status/1" do
    test "returns :up when the worker owns a live AMQP channel" do
      stub_successful_init()

      worker = start_worker!(0)
      assert_worker_connected(0)

      assert Worker.get_amqp_channel_status(worker) == :up
    end

    test "returns :down when the worker has no AMQP channel" do
      worker = start_failed_worker!(0)

      assert Worker.get_amqp_channel_status(worker) == :down
    end

    test "returns :down when the AMQP channel process is not alive" do
      worker = start_failed_worker!(0)

      put_channel(worker, channel_fixture(dead_process(), self()))

      assert Worker.get_amqp_channel_status(worker) == :down
    end

    test "returns :down when the AMQP connection process is not alive" do
      worker = start_failed_worker!(0)

      put_channel(worker, channel_fixture(self(), dead_process()))

      assert Worker.get_amqp_channel_status(worker) == :down
    end
  end

  defp start_worker_supervisors! do
    start_supervised!(
      {Horde.Registry, [keys: :unique, name: EventsProducer.Registry, members: :auto]}
    )

    start_supervised!(
      {Horde.DynamicSupervisor, strategy: :one_for_one, name: Worker.Supervisor, members: :auto}
    )
  end

  defp start_worker!(queue_index) do
    worker_args = [
      queue_name: worker_queue_name(queue_index),
      queue_index: queue_index,
      events_exchange_name: "",
      connection: MockAMQPConnection,
      reconnection_backoff_ms: @reconnection_backoff_ms
    ]

    {:ok, pid} = DynamicSupervisor.start_child(Worker.Supervisor, {Worker, worker_args})
    pid
  end

  defp start_failed_worker!(queue_index) do
    test_process = self()

    stub(MockAMQPConnection, :init, fn %State{queue_index: index} ->
      send(test_process, {:channel_init_failed, index})
      {:error, :channel_init_failed}
    end)

    worker = start_worker!(queue_index)
    assert_worker_disconnected(queue_index)

    worker
  end

  defp assert_worker_connected(queue_index) do
    expected_queue_name = worker_queue_name(queue_index)
    assert_receive {:queue_declared, ^expected_queue_name}, 1_000
  end

  defp assert_worker_disconnected(queue_index) do
    assert_receive {:channel_init_failed, ^queue_index}, 1_000
  end

  defp stub_successful_init do
    stub(MockAMQPConnection, :init, fn _state -> {:ok, channel_fixture()} end)
  end

  defp put_channel(worker, channel) do
    :sys.replace_state(worker, fn state -> %{state | channel: channel} end)
  end

  defp channel_fixture(channel_pid \\ nil, connection_pid \\ nil) do
    %Channel{
      pid: channel_pid || live_process(),
      conn: %Connection{pid: connection_pid || self()}
    }
  end

  defp live_process do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
    end)
  end

  defp dead_process do
    pid = spawn(fn -> :ok end)
    monitor_ref = Process.monitor(pid)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :normal}
    pid
  end

  defp worker_queue_name(queue_index) do
    "healthcheck_#{queue_index}"
  end
end
