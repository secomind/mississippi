# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.MessageTracker.Test do
  use EfxCase, async: false

  import Hammox

  alias AMQP.Channel
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.MessageTracker
  alias Mississippi.Consumer.MessageTracker.Server.State
  alias Mississippi.Consumer.Test.Placeholder

  require Logger

  @moduletag :unit

  setup_all do
    start_supervised({Registry, [keys: :unique, name: Registry.MessageTracker]})

    start_supervised({DynamicSupervisor, strategy: :one_for_one, name: MessageTracker.Supervisor})

    :ok
  end

  doctest Mississippi.Consumer.MessageTracker

  describe "MessageTracker works as an Orleans grain:" do
    setup :create_sharding_key

    @tag :message_tracker_orleans
    test "a process is successfully started with a given sharding key", %{
      sharding_key: sharding_key
    } do
      {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

      mt_processes =
        Registry.select(Registry.MessageTracker, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, pid} in mt_processes
    end

    @tag :message_tracker_orleans
    test "a process is not duplicated when using the same sharding key", %{
      sharding_key: sharding_key
    } do
      {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

      mt_processes =
        Registry.select(Registry.MessageTracker, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, pid} in mt_processes

      {:ok, ^pid} = MessageTracker.get_message_tracker(sharding_key)
      assert {{:sharding_key, sharding_key}, pid} in mt_processes
    end

    @tag :message_tracker_orleans
    test "a process is spawned again if requested after termination", %{
      sharding_key: sharding_key
    } do
      {:ok, first_pid} = MessageTracker.get_message_tracker(sharding_key)

      mt_processes =
        Registry.select(Registry.MessageTracker, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, first_pid} in mt_processes

      DynamicSupervisor.terminate_child(MessageTracker.Supervisor, first_pid)

      {:ok, second_pid} = MessageTracker.get_message_tracker(sharding_key)
      assert first_pid != second_pid

      mt_processes =
        Registry.select(Registry.MessageTracker, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, second_pid} in mt_processes
      refute {{:sharding_key, sharding_key}, first_pid} in mt_processes
    end
  end

  describe "MessageTracker fault tolerance:" do
    setup :setup_mock_message_handler
    setup :create_sharding_key
    setup :create_message
    setup :setup_mock_channel
    setup :setup_mock_data_updater
    setup :add_on_exit

    @tag :message_tracker_fault_tolerance
    test "a process crashes if the related AMQP Channel crashes", %{
      sharding_key: sharding_key,
      channel: channel,
      data_updater: data_updater,
      message: message
    } do
      {:ok, message_tracker_pid} = MessageTracker.get_message_tracker(sharding_key)
      mt_ref = Process.monitor(message_tracker_pid)

      MessageTracker.handle_message(message_tracker_pid, message, channel)

      bind(DataUpdater, :get_data_updater_process, fn _ -> {:ok, data_updater} end)

      send(message_tracker_pid, {:DOWN, :dontcare, :process, channel.pid, :crash})

      assert_receive {:DOWN, ^mt_ref, :process, ^message_tracker_pid, :channel_crashed}

      refute Process.alive?(message_tracker_pid)
    end

    @tag :message_tracker_fault_tolerance
    test "a process resends the message if the related DataUpdater crashes", %{
      sharding_key: sharding_key,
      channel: channel,
      message: message
    } do
      {:ok, message_tracker_pid} = MessageTracker.get_message_tracker(sharding_key)
      :erlang.trace(message_tracker_pid, true, [:receive])

      data_updater_1_pid = get_mock_data_updater!()

      bind(DataUpdater, :get_data_updater_process, fn _ -> {:ok, data_updater_1_pid} end, calls: 1)

      MessageTracker.handle_message(message_tracker_pid, message, channel)

      assert_receive {:trace, ^data_updater_1_pid, :receive, {_, {:handle_message, ^message}}}

      kill_data_updater(data_updater_1_pid)

      assert_receive {:trace, ^message_tracker_pid, :receive, {:DOWN, _, :process, ^data_updater_1_pid, :normal}}

      data_updater_2_pid = get_mock_data_updater!()

      bind(DataUpdater, :get_data_updater_process, fn _ -> {:ok, data_updater_2_pid} end, calls: 1)

      assert_receive {:trace, ^data_updater_2_pid, :receive, {_, {:handle_message, ^message}}}
    end
  end

  describe "MessageTracker message handling:" do
    setup :setup_mock_message_handler
    setup :create_sharding_key
    setup :create_message
    setup :setup_mock_channel
    setup :add_on_exit

    @tag :message_tracker_message_handling
    test "is successful on valid message", %{
      sharding_key: sharding_key,
      channel: channel,
      message: message
    } do
      {:ok, message_tracker_pid} = MessageTracker.get_message_tracker(sharding_key)
      :erlang.trace(message_tracker_pid, true, [:receive])

      {:ok, data_updater_pid} =
        GenServer.start_link(DataUpdater,
          sharding_key: sharding_key,
          message_handler: MockMessageHandler
        )

      :erlang.trace(data_updater_pid, true, [:receive])

      bind(DataUpdater, :get_data_updater_process, fn _ -> {:ok, data_updater_pid} end, calls: 1)

      MessageTracker.handle_message(message_tracker_pid, message, channel)

      assert_receive {:trace, ^data_updater_pid, :receive, {_, {:handle_message, ^message}}}

      bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, message_tracker_pid} end, calls: 1)

      assert_receive {:trace, ^message_tracker_pid, :receive, {_, {_, _}, {:ack_delivery, ^message}}}
    end

    @tag :message_tracker_message_handling
    test "respects ordering of messages", %{
      sharding_key: sharding_key,
      channel: channel
    } do
      message_1 = message_fixture(sharding_key)
      message_2 = message_fixture(sharding_key)
      {:ok, message_tracker_pid} = MessageTracker.get_message_tracker(sharding_key)
      :erlang.trace(message_tracker_pid, true, [:receive])
      bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, message_tracker_pid} end)

      {:ok, data_updater_pid} =
        GenServer.start_link(DataUpdater,
          sharding_key: sharding_key,
          message_handler: MockMessageHandler
        )

      :erlang.trace(data_updater_pid, true, [:receive])
      bind(DataUpdater, :get_data_updater_process, fn _ -> {:ok, data_updater_pid} end)

      MessageTracker.handle_message(message_tracker_pid, message_1, channel)
      MessageTracker.handle_message(message_tracker_pid, message_2, channel)
      message_queue = :queue.from_list([message_1, message_2])

      assert %State{queue: ^message_queue} = :sys.get_state(message_tracker_pid)

      assert_receive {:trace, ^data_updater_pid, :receive, {_, {:handle_message, ^message_1}}}

      refute_received {:trace, ^data_updater_pid, :receive, {_, {:handle_message, ^message_2}}}

      assert_receive {:trace, ^message_tracker_pid, :receive,
                      {:"$gen_call", {^data_updater_pid, _}, {:ack_delivery, ^message_1}}}

      refute_received {:trace, ^message_tracker_pid, :receive,
                       {:"$gen_call", {^data_updater_pid, _}, {:ack_delivery, ^message_2}}}

      # According to :queue, :queue.drop(:queue.from_list([message_1, message_2])) != :queue.from_list([message_2])
      queue_2 = :queue.drop(message_queue)

      assert %State{queue: ^queue_2} = :sys.get_state(message_tracker_pid)

      assert_receive {:trace, ^data_updater_pid, :receive, {_, {:handle_message, ^message_2}}}

      assert_receive {:trace, ^message_tracker_pid, :receive,
                      {:"$gen_call", {^data_updater_pid, _}, {:ack_delivery, ^message_2}}}
    end

    test "shuts down if message forces process termination", %{
      sharding_key: sharding_key,
      channel: channel,
      message: message
    } do
      Process.flag(:trap_exit, true)

      {:ok, message_tracker_pid} = MessageTracker.get_message_tracker(sharding_key)
      :erlang.trace(message_tracker_pid, true, [:receive])
      mt_ref = Process.monitor(message_tracker_pid)

      MockMessageHandler
      |> expect(:init, fn _ -> {:ok, []} end)
      |> expect(:handle_message, fn _, _, _, _, _ ->
        {:stop, :some_reason, :ack, []}
      end)
      |> expect(:terminate, fn _, _ -> :ok end)

      {:ok, data_updater_pid} =
        GenServer.start_link(DataUpdater,
          sharding_key: sharding_key,
          message_handler: MockMessageHandler
        )

      du_ref = Process.monitor(data_updater_pid)

      bind(DataUpdater, :get_data_updater_process, fn _ -> {:ok, data_updater_pid} end, calls: 1)

      MessageTracker.handle_message(message_tracker_pid, message, channel)

      bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, message_tracker_pid} end, calls: 1)

      assert_receive {:trace, ^message_tracker_pid, :receive, {_, {_, _}, {:ack_delivery, ^message}}}

      assert_receive {:DOWN, ^du_ref, :process, ^data_updater_pid, {:shutdown, :requested}}
      refute Process.alive?(data_updater_pid)

      assert_receive {:DOWN, ^mt_ref, :process, ^message_tracker_pid, {:shutdown, :requested}}
      refute Process.alive?(message_tracker_pid)
    end
  end

  defp setup_mock_message_handler(_context) do
    MockMessageHandler
    |> stub(:init, fn _ -> {:ok, []} end)
    |> stub(:handle_message, fn _, _, _, _, _ -> {:ack, :ok, []} end)

    Hammox.set_mox_global()
  end

  defp create_sharding_key(context) do
    Map.put(context, :sharding_key, "sharding_#{System.unique_integer()}")
  end

  defp create_message(context) do
    %{sharding_key: sharding_key} = context
    message = message_fixture(sharding_key)

    Map.put(context, :message, message)
  end

  defp add_on_exit(context) do
    if context[:channel] do
      on_exit(fn -> Process.exit(context.channel.pid, :kill) end)
    end

    if context[:data_updater] do
      on_exit(fn -> Process.exit(context.data_updater, :kill) end)
    end

    context
  end

  defp setup_mock_channel(context) do
    {:ok, pid} = GenServer.start(Placeholder, [])

    channel = %Channel{pid: pid}

    Map.put(context, :channel, channel)
  end

  defp setup_mock_data_updater(context) do
    data_updater = get_mock_data_updater!()

    Map.put(context, :data_updater, data_updater)
  end

  defp get_mock_data_updater! do
    {:ok, data_updater} = GenServer.start(Placeholder, [])

    :erlang.trace(data_updater, true, [:receive])

    data_updater
  end

  defp kill_data_updater(data_updater) do
    GenServer.cast(data_updater, :die)
  end

  defp message_fixture(sharding_key) do
    %Mississippi.Consumer.Message{
      payload: "payload_#{System.unique_integer()}",
      headers: %{"sharding_key" => :erlang.term_to_binary(sharding_key)},
      timestamp: DateTime.utc_now(),
      meta: %{
        message_id: "#{System.unique_integer()}",
        delivery_tag: System.unique_integer()
      }
    }
  end
end
