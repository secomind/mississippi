# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.AMQPDataConsumer.Test do
  use ExUnit.Case, async: false
  use Mimic

  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Exchange
  alias Horde.Registry
  alias Mississippi.Consumer.AMQPDataConsumer
  alias Mississippi.Consumer.AMQPDataConsumer.State
  alias Mississippi.Consumer.MessageTracker
  alias Mississippi.Consumer.Test.Placeholder
  alias Mississippi.Producer.EventsProducer.AMQPConnection

  require Logger
  require Mimic

  @moduletag :integration

  doctest Mississippi.Consumer.AMQPDataConsumer

  setup_all do
    start_supervised({Registry, [keys: :unique, name: AMQPDataConsumer.Registry]})
    exchange_name = create_exchange()
    %{exchange_name: exchange_name}
  end

  describe "AMQPDataConsumer message handling:" do
    setup :create_queue_index
    setup :create_sharding_key
    setup :create_payload
    setup :create_meta
    setup :setup_mock_message_tracker
    setup :setup_mox
    setup :start_consumer

    @tag :data_consumer_message_handling
    test "Messages are forwarded to different trackers based on sharding key", context do
      data_consumer_pid = context.data_consumer_pid

      sharding_key_1 = get_sharding_key()
      sharding_key_2 = get_sharding_key()

      tracker_1 = get_message_tracker()
      tracker_2 = get_message_tracker()

      trackers = %{
        sharding_key_1 => tracker_1,
        sharding_key_2 => tracker_2
      }

      expect(MessageTracker, :get_message_tracker, 1, fn _ ->
        {:ok, trackers[sharding_key_1]}
      end)

      payload_1 = get_payload()
      meta_1 = meta_fixture(sharding_key_1)

      send(data_consumer_pid, {:basic_deliver, payload_1, meta_1})

      assert_receive {:trace, ^tracker_1, :receive, {_, {_, message_1, _}}}
      refute_receive {:trace, ^tracker_2, :receive, _}
      assert message_1.payload == payload_1
      assert sharding_key_from(message_1) == sharding_key_1

      payload_2 = get_payload()
      meta_2 = meta_fixture(sharding_key_2)

      expect(MessageTracker, :get_message_tracker, 1, fn _ ->
        {:ok, trackers[sharding_key_2]}
      end)

      send(data_consumer_pid, {:basic_deliver, payload_2, meta_2})

      assert_receive {:trace, ^tracker_2, :receive, {_, {_, message_2, _}}}
      refute_receive {:trace, ^tracker_1, :receive, _}
      assert message_2.payload == payload_2
      assert sharding_key_from(message_2) == sharding_key_2
    end

    @tag :data_consumer_message_handling
    test "AMQPDataConsumer monitors a MessageTracker when starting to forward messages", %{
      data_consumer_pid: data_consumer_pid,
      message_tracker: message_tracker,
      payload: payload,
      meta: meta
    } do
      expect(MessageTracker, :get_message_tracker, 1, fn _ -> {:ok, message_tracker} end)

      send(data_consumer_pid, {:basic_deliver, payload, meta})

      assert %State{monitors: [^message_tracker]} = :sys.get_state(data_consumer_pid)
    end
  end

  describe "AMQPDataConsumer fault tolerance:" do
    setup :create_queue_index
    setup :create_sharding_key
    setup :create_payload
    setup :create_meta
    setup :setup_mock_message_tracker
    setup :setup_mox
    setup :start_consumer

    @tag :data_consumer_fault_tolerance
    test "a process stays up when a monitored MessageTracker exits normally", %{
      data_consumer_pid: data_consumer_pid,
      message_tracker: message_tracker,
      payload: payload,
      meta: meta
    } do
      :erlang.trace(data_consumer_pid, true, [:receive])

      expect(MessageTracker, :get_message_tracker, 1, fn _ -> {:ok, message_tracker} end)

      send(data_consumer_pid, {:basic_deliver, payload, meta})

      assert %State{monitors: [^message_tracker]} = :sys.get_state(data_consumer_pid)

      kill_message_tracker(message_tracker)

      assert_receive {:trace, ^data_consumer_pid, :receive, {:DOWN, _, :process, ^message_tracker, :normal}}

      assert Process.alive?(data_consumer_pid)

      assert %State{monitors: []} = :sys.get_state(data_consumer_pid)
    end

    @tag :data_consumer_fault_tolerance
    test "a process crashes if the related MessageTracker crashes", %{
      data_consumer_pid: data_consumer_pid,
      message_tracker: message_tracker,
      payload: payload,
      meta: meta
    } do
      Process.flag(:trap_exit, true)

      consumer_ref = Process.monitor(data_consumer_pid)

      expect(MessageTracker, :get_message_tracker, 1, fn _ -> {:ok, message_tracker} end)

      send(data_consumer_pid, {:basic_deliver, payload, meta})

      assert %State{monitors: [^message_tracker]} = :sys.get_state(data_consumer_pid)

      send(data_consumer_pid, {:DOWN, :dontcare, :process, message_tracker, :crash})

      assert_receive {:DOWN, ^consumer_ref, :process, ^data_consumer_pid, _}

      refute Process.alive?(data_consumer_pid)
    end
  end

  defp sharding_key_from(message) do
    :erlang.binary_to_term(message.headers["sharding_key"])
  end

  defp setup_mox(context) do
    mock_channel = %Channel{pid: self()}

    stub(AMQPConnection, :init, fn _, _ -> {:ok, mock_channel} end)
    context
  end

  defp create_queue_index(context) do
    index = System.unique_integer([:positive])

    Map.put(context, :queue_index, index)
  end

  defp setup_mock_message_tracker(context) do
    message_tracker = get_message_tracker()
    Map.put(context, :message_tracker, message_tracker)
  end

  defp get_payload do
    "payload_#{System.unique_integer()}"
  end

  defp create_payload(context) do
    Map.put(context, :payload, get_payload())
  end

  defp create_meta(context) do
    %{sharding_key: sharding_key} = context

    Map.put(context, :meta, meta_fixture(sharding_key))
  end

  defp meta_fixture(sharding_key) do
    %{
      headers: [
        {"sharding_key", "binary", :erlang.term_to_binary(sharding_key)}
      ],
      timestamp: DateTime.utc_now()
    }
  end

  defp amqp_data_consumer_fixture(exchange_name, queue_index) do
    start_link_args =
      [
        exchange_name: exchange_name,
        queue_index: queue_index,
        queue_name: "queue_#{queue_index}",
        connection_options: [host: "localhost"]
      ]

    %{
      id: {AMQPDataConsumer, queue_index},
      start: {AMQPDataConsumer, :start_link, [start_link_args]}
    }
  end

  defp start_amqp_data_consumer!(exchange_name, queue_index) do
    data_consumer = exchange_name |> amqp_data_consumer_fixture(queue_index) |> start_supervised!()
    allow(MessageTracker, self(), data_consumer)
    data_consumer
  end

  defp get_sharding_key do
    "sharding_#{System.unique_integer()}"
  end

  defp create_sharding_key(context) do
    Map.put(context, :sharding_key, get_sharding_key())
  end

  defp get_message_tracker do
    {:ok, message_tracker} = GenServer.start(Placeholder, [])
    :erlang.trace(message_tracker, true, [:receive])

    message_tracker
  end

  def kill_message_tracker(message_tracker) do
    GenServer.cast(message_tracker, :die)
  end

  defp start_consumer(context) do
    %{queue_index: queue_index, exchange_name: exchange_name} = context
    data_consumer_pid = start_amqp_data_consumer!(exchange_name, queue_index)
    %{data_consumer_pid: data_consumer_pid}
  end

  defp create_exchange do
    exchange = "mississippi_#{System.unique_integer([:positive])}"

    {:ok, conn} = Connection.open(host: "localhost")
    {:ok, channel} = Channel.open(conn)
    :ok = Exchange.declare(channel, exchange, :direct)
    Channel.close(channel)
    Connection.close(conn)

    exchange
  end
end
