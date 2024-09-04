# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.DataUpdater.Test do
  use EfxCase, async: false

  import Hammox

  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.DataUpdater.State
  alias Mississippi.Consumer.MessageTracker
  alias Mississippi.Consumer.Test.Placeholder

  @moduletag :unit

  setup_all do
    start_supervised!({Registry, [keys: :unique, name: Registry.DataUpdater]})

    start_supervised!({DataUpdater.Supervisor, message_handler: MockMessageHandler})

    :ok
  end

  doctest Mississippi.Consumer.DataUpdater

  describe "DataUpdater works as an Orleans grain:" do
    setup :setup_mock_message_handler
    setup :create_sharding_key

    @tag :data_updater_orleans
    test "a process is successfully started with a given sharding key", %{
      sharding_key: sharding_key
    } do
      {:ok, pid} = DataUpdater.get_data_updater_process(sharding_key)

      du_processes =
        Registry.select(Registry.DataUpdater, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, pid} in du_processes
    end

    @tag :data_updater_orleans
    test "a process is not duplicated when using the same sharding key", %{
      sharding_key: sharding_key
    } do
      {:ok, pid} = DataUpdater.get_data_updater_process(sharding_key)

      du_processes =
        Registry.select(Registry.DataUpdater, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, pid} in du_processes

      {:ok, ^pid} = DataUpdater.get_data_updater_process(sharding_key)
      assert {{:sharding_key, sharding_key}, pid} in du_processes
    end

    @tag :data_updater_orleans
    test "a process is spawned again if requested after termination", %{
      sharding_key: sharding_key
    } do
      {:ok, first_pid} = DataUpdater.get_data_updater_process(sharding_key)

      du_processes =
        Registry.select(Registry.DataUpdater, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, first_pid} in du_processes

      DynamicSupervisor.terminate_child(DataUpdater.Supervisor, first_pid)

      {:ok, second_pid} = DataUpdater.get_data_updater_process(sharding_key)
      assert first_pid != second_pid

      du_processes =
        Registry.select(Registry.DataUpdater, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      assert {{:sharding_key, sharding_key}, second_pid} in du_processes
      refute {{:sharding_key, sharding_key}, first_pid} in du_processes
    end
  end

  describe "DataUpdater handles messages:" do
    setup :setup_mock_message_handler
    setup :create_sharding_key
    setup :create_message
    setup :setup_mock_message_tracker
    setup :add_on_exit

    @tag :data_updater_message_handling
    test "acking when ok", %{
      sharding_key: sharding_key,
      message: message,
      message_tracker: message_tracker
    } do
      expect(MockMessageHandler, :handle_message, fn _, _, _, _, state -> {:ack, :ok, state} end)

      {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)

      :erlang.trace(data_updater_pid, true, [:receive])

      bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, message_tracker} end, calls: 1)

      DataUpdater.handle_message(data_updater_pid, message)

      assert_receive {:trace, ^message_tracker, :receive, {_, {_, _}, {:ack_delivery, ^message}}}
    end

    @tag :data_updater_message_handling
    test "rejecting when error", %{
      sharding_key: sharding_key,
      message: message,
      message_tracker: message_tracker
    } do
      expect(MockMessageHandler, :handle_message, fn _, _, _, _, state ->
        {:discard, :aaaa, state}
      end)

      {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)

      :erlang.trace(data_updater_pid, true, [:receive])

      bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, message_tracker} end, calls: 1)

      DataUpdater.handle_message(data_updater_pid, message)

      assert_receive {:trace, ^message_tracker, :receive, {_, {_, _}, {:reject, ^message}}}
    end

    @tag :data_updater_message_handling
    test "terminating when requested", %{
      sharding_key: sharding_key,
      message: message,
      message_tracker: message_tracker
    } do
      Process.flag(:trap_exit, true)

      expect(MockMessageHandler, :handle_message, fn _, _, _, _, state ->
        {:stop, :some_reason, :ack, state}
      end)

      expect(MockMessageHandler, :terminate, fn _, _ -> :ok end)

      {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)

      ref = Process.monitor(data_updater_pid)
      bind(MessageTracker, :get_message_tracker, fn _ -> {:ok, message_tracker} end, calls: 1)

      DataUpdater.handle_message(data_updater_pid, message)

      assert_receive {:DOWN, ^ref, :process, ^data_updater_pid, {:shutdown, :requested}}
      refute Process.alive?(data_updater_pid)
    end
  end

  describe "DataUpdater handles signals:" do
    setup :setup_mock_message_handler
    setup :create_sharding_key
    setup :create_signal

    @tag :data_updater_signal_handling
    test "updating the handler state", %{
      sharding_key: sharding_key,
      signal: signal
    } do
      expect(MockMessageHandler, :init, fn _sharding_key ->
        {:ok, %{signals: []}}
      end)

      expect(MockMessageHandler, :handle_signal, fn signal, _state ->
        {:ok, %{signals: [signal]}}
      end)

      {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)

      assert %State{handler_state: %{signals: []}} = :sys.get_state(data_updater_pid)

      DataUpdater.handle_signal(data_updater_pid, signal)

      assert %State{handler_state: %{signals: [^signal]}} = :sys.get_state(data_updater_pid)
    end
  end

  defp setup_mock_message_handler(_context) do
    stub(MockMessageHandler, :init, fn _key -> {:ok, []} end)

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

  defp create_signal(context) do
    signal = System.unique_integer([])

    Map.put(context, :signal, signal)
  end

  defp add_on_exit(context) do
    if context[:message_tracker] do
      on_exit(fn -> Process.exit(context.message_tracker, :kill) end)
    end

    context
  end

  defp setup_mock_message_tracker(context) do
    {:ok, message_tracker} = GenServer.start(Placeholder, [])

    :erlang.trace(message_tracker, true, [:receive])
    Map.put(context, :message_tracker, message_tracker)
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
