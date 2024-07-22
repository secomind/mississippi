defmodule Mississippi.Consumer.MessageTracker.Test do
  use ExUnit.Case

  alias Mississippi.Consumer.MessageTracker

  require Logger

  setup_all do
    start_supervised!({Registry, [keys: :unique, name: Registry.MessageTracker]})

    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: MessageTracker.Supervisor})

    :ok
  end

  doctest Mississippi.Consumer.MessageTracker

  test "a MessageTracker process is successfully started with a given sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

    mt_processes = Registry.select(Registry.MessageTracker, [{{:"$1", :_, :_}, [], [:"$1"]}])

    assert [{:sharding_key, ^sharding_key}] = mt_processes

    on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, pid) end)
  end

  test "a MessageTracker process is not duplicated when using the same sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

    mt_processes = Registry.select(Registry.MessageTracker, [{{:"$1", :_, :_}, [], [:"$1"]}])
    assert [{:sharding_key, ^sharding_key}] = mt_processes

    {:ok, ^pid} = MessageTracker.get_message_tracker(sharding_key)
    assert [{:sharding_key, ^sharding_key}] = mt_processes

    on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, pid) end)
  end

  test "a MessageTracker process is spawned again if requested after termination" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, first_pid} = MessageTracker.get_message_tracker(sharding_key)

    mt_processes =
      Registry.select(Registry.MessageTracker, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    assert [{{:sharding_key, ^sharding_key}, ^first_pid}] = mt_processes

    DynamicSupervisor.terminate_child(MessageTracker.Supervisor, first_pid)
    # Give some time to the Registry to delete the entry
    Process.sleep(:timer.seconds(2))
    [] = Registry.select(Registry.MessageTracker, [{{:"$1", :_, :_}, [], [:"$1"]}])

    {:ok, second_pid} = MessageTracker.get_message_tracker(sharding_key)
    assert first_pid != second_pid

    mt_processes =
      Registry.select(Registry.MessageTracker, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    assert [{{:sharding_key, ^sharding_key}, ^second_pid}] = mt_processes

    on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, second_pid) end)
  end
end
