defmodule Mississippi.Consumer.AMQPDataConsumer.Test do
  use EfxCase

  alias Mississippi.Consumer.AMQPDataConsumer
  alias Mississippi.Consumer.MessageTracker
  alias AMQP.Channel

  require Logger

  # We use Mox here because we don't care about the type safety
  # guarantees of Hammox
  import Mox

  setup_all do
    start_supervised!({Registry, [keys: :unique, name: Registry.AMQPDataConsumer]})

    mock_channel = %Channel{pid: self()}

    MockAMQPConnection
    |> stub(:init, fn _ -> {:ok, mock_channel} end)
    |> stub(:adapter, fn -> ExRabbitPool.FakeRabbitMQ end)

    Mox.set_mox_global()

    :ok
  end

  doctest Mississippi.Consumer.AMQPDataConsumer

  setup do
    process_1 = Process.spawn(&wait_for_message/0, [])
    :erlang.trace(process_1, true, [:receive])

    process_2 = Process.spawn(&wait_for_message/0, [])
    :erlang.trace(process_2, true, [:receive])

    %{
      sharding_key_1: process_1,
      sharding_key_2: process_2
    }
  end

  test "Messages are forwarded to different trackers based on sharding key", %{
    sharding_key_1: tracker_1,
    sharding_key_2: tracker_2
  } do
    data_consumer_pid =
      start_supervised!({AMQPDataConsumer, queue_index: 0, queue_name: "queue_0"})

    sharding_key_1 = :sharding_key_1
    sharding_key_2 = :sharding_key_2

    bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, tracker_1} end, calls: 1)
    bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, tracker_2} end, calls: 1)

    payload_1 = "payload_#{System.unique_integer()}"
    meta_1 = meta_fixture(sharding_key_1)

    send(data_consumer_pid, {:basic_deliver, payload_1, meta_1})

    assert_receive {:trace, ^tracker_1, :receive, {_, {_, message_1, _}}}
    refute_receive {:trace, ^tracker_2, :receive, _}
    assert message_1.payload == payload_1
    assert sharding_key_from(message_1) == sharding_key_1

    payload_2 = "payload_#{System.unique_integer()}"
    meta_2 = meta_fixture(sharding_key_2)

    send(data_consumer_pid, {:basic_deliver, payload_2, meta_2})

    assert_receive {:trace, ^tracker_2, :receive, {_, {_, message_2, _}}}
    refute_receive {:trace, ^tracker_1, :receive, _}
    assert message_2.payload == payload_2
    assert sharding_key_from(message_2) == sharding_key_2
  end

  defp meta_fixture(sharding_key) do
    %{
      headers: [
        {"sharding_key", "binary", :erlang.term_to_binary(sharding_key)}
      ],
      timestamp: DateTime.utc_now()
    }
  end

  defp wait_for_message do
    receive do
      x -> x
    end
  end

  defp sharding_key_from(message) do
    :erlang.binary_to_term(message.headers["sharding_key"])
  end
end
