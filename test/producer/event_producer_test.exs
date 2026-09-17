# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.Test do
  use ExUnit.Case

  import Hammox

  alias AMQP.Channel
  alias AMQP.Connection
  alias Horde.Registry
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.State
  alias Mississippi.Producer.EventsProducer.Worker
  alias Mississippi.Producer.ProducersSupervisor

  require Logger

  @moduletag :unit

  setup do
    Hammox.set_mox_global()
    test_process = self()

    start_supervised!({FakeConnectionAdapter, test_process})

    MockAMQPConnection
    |> stub(:init, fn _ -> {:ok, channel_fixture()} end)
    |> stub(:adapter, fn -> FakeConnectionAdapter end)

    total_count = 8

    events_producer_supervisor_pid =
      start_supervised!(events_producer_supervisor_fixture(total_count))

    producers = event_producer_pids(total_count)

    for {_id, pid} <- producers, do: :erlang.trace(pid, true, [:receive])

    %{
      events_producer_supervisor_pid: events_producer_supervisor_pid,
      producers: producers,
      total_count: total_count
    }
  end

  describe "EventsProducer.publish/3 via router" do
    @tag :events_producer_message_handling
    test "publishes the message to the correct shard worker when connected", context do
      %{total_count: total_count} = context
      sharding_key = 42
      expected_index = :erlang.phash2(sharding_key, total_count)
      expected_queue = "#{expected_index}"

      valid_payload = "payload-#{System.unique_integer([:positive])}"
      valid_opts = [sharding_key: sharding_key]

      assert :ok == EventsProducer.publish(valid_payload, valid_opts)

      assert_receive {:published, _channel, ^expected_queue, ^valid_payload}, 500
    end

    @tag :events_producer_message_handling
    test "returns an error when it fails to connect to a channel", context do
      %{producers: producers} =
        context

      test_process = self()
      valid_payload = payload_fixture()
      valid_opts = publish_options_fixture()
      sharding_key = valid_opts[:sharding_key]
      {index, producer} = producer_for_sharding_key(producers, sharding_key)

      stub(MockAMQPConnection, :init, fn _ ->
        send(test_process, :reconnecting)
        {:error, :event_producer_init_fail}
      end)

      Process.exit(producer, :kill)
      assert_receive :reconnecting
      _ = event_producer_pid(index)

      assert {:error, :reconnecting} ==
               EventsProducer.publish(valid_payload, valid_opts)
    end

    @tag :events_producer_message_handling
    test "returns an error when reconnecting", context do
      %{producers: producers} = context
      valid_payload = payload_fixture()
      valid_opts = publish_options_fixture()

      sharding_key = valid_opts[:sharding_key]
      {queue_index, producer} = producer_for_sharding_key(producers, sharding_key)
      %State{channel: %{pid: channel_pid}} = :sys.get_state(producer)
      test_process = self()

      stub(MockAMQPConnection, :init, fn _ ->
        send(test_process, :channel_init)
        {:error, :event_producer_init_fail}
      end)

      Process.exit(channel_pid, :kill)
      assert_receive :channel_init

      new_producer_pid = event_producer_pid(queue_index)

      %State{channel: nil} = :sys.get_state(new_producer_pid)

      assert {:error, :reconnecting} ==
               EventsProducer.publish(valid_payload, valid_opts)
    end
  end

  @tag :events_producer_fault_tolerance
  test "reconnects if the AMQP connection goes down", context do
    %{producers: producers} = context
    test_process = self()

    stub(MockAMQPConnection, :init, fn _ ->
      send(test_process, :channel_init)
      {:error, :event_producer_init_fail}
    end)

    {index, producer_pid} = Enum.random(producers)
    %State{channel: %{pid: channel_pid}} = :sys.get_state(producer_pid)

    Process.exit(channel_pid, :kill)
    assert_receive :channel_init

    producer_pid = event_producer_pid(index)

    assert %State{channel: nil} = :sys.get_state(producer_pid)

    reconnected_message = :events_producer_reconnected

    expect(MockAMQPConnection, :init, fn _ ->
      # send a message to the test process to signal that
      # the events producer tried to (re)initialize the connection
      send(test_process, reconnected_message)

      {:ok, channel_fixture(test_process)}
    end)

    assert_receive ^reconnected_message

    assert %State{channel: %{pid: ^test_process}} = :sys.get_state(producer_pid)
  end

  describe "EventsProducer's initialization" do
    @tag :events_producer_initialization
    test "fails when the sharding key is not specified" do
      valid_payload = "payload-#{System.unique_integer([:positive])}"

      assert_raise NimbleOptions.ValidationError, fn ->
        EventsProducer.publish(valid_payload, [])
      end
    end

    test "fails when the events producer config is not found" do
      Registry.unregister(EventsProducer.Registry, :events_producer_config)

      # await registry update
      retry(10, fn ->
        Registry.lookup(EventsProducer.Registry, :events_producer_config) == []
      end)

      valid_payload = "payload-#{System.unique_integer([:positive])}"

      assert {:error, :events_producer_uninitialized} ==
               EventsProducer.publish(valid_payload, sharding_key: 123)
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

  defp payload_fixture do
    "payload-#{System.unique_integer([:positive])}"
  end

  defp publish_options_fixture do
    [sharding_key: System.unique_integer()]
  end

  defp events_producer_supervisor_fixture(total_count) do
    opts = [
      queues: [
        total_count: total_count,
        ssl_options: [verify: :verify_none],
        events_exchange_name: "",
        total_count: System.unique_integer([:positive]),
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

  defp producer_for_sharding_key(producers, sharding_key) do
    total_count = Enum.count(producers)
    index = :erlang.phash2(sharding_key, total_count)
    Enum.find_value(producers, fn {id, pid} -> id == index && {index, pid} end)
  end

  defp retry(count, message \\ "check failed", check_fun) do
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
