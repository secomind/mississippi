defmodule Mississippi.Producer.EventsProducer.Test do
  use ExUnit.Case

  import Hammox

  alias AMQP.Channel
  alias AMQP.Connection
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.State

  @moduletag :unit

  setup do
    Hammox.set_mox_global()
    test_process = self()
    temporary_pid = temporary_process()

    MockAMQPConnection
    |> stub(:init, fn _ -> {:ok, channel_fixture(temporary_pid)} end)
    |> stub(:adapter, fn ->
      FakeConnectionAdapter.start(test_process)
      FakeConnectionAdapter
    end)

    events_producer_pid =
      start_supervised!(events_producer_fixture())

    :erlang.trace(events_producer_pid, true, [:receive])

    %{events_producer_pid: events_producer_pid, channel_pid: temporary_pid}
  end

  describe "EventsProducer.publish/2" do
    @tag :events_producer_message_handling
    test "publishes the message to the channel when connected" do
      valid_payload = payload_fixture()
      valid_opts = publish_options_fixture()

      EventsProducer.publish(valid_payload, valid_opts)

      assert_receive {:published, _, _, ^valid_payload}
    end

    @tag :events_producer_message_handling
    test "returns an error when it fails to connect to a channel" do
      # we need to start a new events producer
      stop_supervised!(EventsProducer)

      stub(MockAMQPConnection, :init, fn _ -> {:error, :event_producer_init_fail} end)
      start_supervised!(events_producer_fixture())

      valid_payload = payload_fixture()
      valid_opts = publish_options_fixture()

      assert {:error, :reconnecting} ==
               EventsProducer.publish(valid_payload, valid_opts)
    end

    @tag :events_producer_message_handling
    test "returns an error when reconnecting", args do
      stub(MockAMQPConnection, :init, fn _ -> {:error, :event_producer_init_fail} end)
      stop_and_await_channel(args.channel_pid)

      %State{channel: nil} = :sys.get_state(args.events_producer_pid)

      valid_payload = payload_fixture()
      valid_opts = publish_options_fixture()

      assert {:error, :reconnecting} ==
               EventsProducer.publish(valid_payload, valid_opts)
    end
  end

  @tag :events_producer_fault_tolerance
  test "reconnects if the AMQP connection goes down", args do
    stub(MockAMQPConnection, :init, fn _ -> {:error, :event_producer_init_fail} end)
    stop_and_await_channel(args.channel_pid)

    assert %State{channel: nil} = :sys.get_state(args.events_producer_pid)

    me = self()
    reconnected_message = :events_producer_reconnected

    expect(MockAMQPConnection, :init, fn _ ->
      # send a message to the test process to signal that
      # the events producer tried to (re)initialize the connection
      send(me, reconnected_message)

      {:ok, channel_fixture(me)}
    end)

    # wait for events producer to at least call connection.init
    receive do
      ^reconnected_message -> :ok
    after
      100 -> raise "Events producer did not reconnect"
    end

    assert %State{channel: %{pid: ^me}} = :sys.get_state(args.events_producer_pid)
  end

  describe "EventsProducer's initialization" do
    @tag :events_producer_initialization
    test "fails when the sharding key is not specified" do
      valid_payload = payload_fixture()

      assert_raise NimbleOptions.ValidationError, fn ->
        EventsProducer.publish(valid_payload, [])
      end
    end

    @tag :events_producer_initialization
    test "fails when the timestamp is not an integer" do
      valid_payload = payload_fixture()

      opts =
        Keyword.put(publish_options_fixture(), :timestamp, DateTime.utc_now())

      assert_raise NimbleOptions.ValidationError, fn ->
        EventsProducer.publish(valid_payload, opts)
      end
    end
  end

  defp temporary_process do
    spawn(fn ->
      receive do
        _ -> nil
      end
    end)
  end

  defp stop_and_await_channel(channel_pid) do
    Process.exit(channel_pid, :kill)

    receive do
      {:trace, _, :receive, {:DOWN, _, _, _, _}} -> :ok
    after
      100 -> raise "Events producer did not receive a :DOWN from channel"
    end
  end

  defp channel_fixture(channel_pid, connection_pid \\ nil) do
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

  defp events_producer_fixture do
    {
      EventsProducer,
      ssl_options: [verify: :verify_none],
      events_exchange_name: "",
      total_count: System.unique_integer([:positive]),
      connection: MockAMQPConnection,
      reconnection_backoff_ms: 0
    }
  end
end
