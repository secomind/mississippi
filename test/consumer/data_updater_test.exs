defmodule Mississippi.Consumer.DataUpdater.Test do
  use ExUnit.Case

  import Hammox

  alias Mississippi.Consumer.DataUpdater

  setup_all do
    start_supervised!({Registry, [keys: :unique, name: Registry.DataUpdater]})

    start_supervised!({DataUpdater.Supervisor, message_handler: MockMessageHandler})

    stub(MockMessageHandler, :init, fn _key -> {:ok, []} end)

    set_mox_global()

    :ok
  end

  doctest Mississippi.Consumer.DataUpdater

  test "a DataUpdater process is successfully started with a given sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = DataUpdater.get_data_updater_process(sharding_key)

    dup_processes = Registry.select(Registry.DataUpdater, [{{:"$1", :_, :_}, [], [:"$1"]}])

    assert [{:sharding_key, ^sharding_key}] = dup_processes

    on_exit(fn -> DataUpdater.Supervisor.terminate_child(pid) end)
  end

  test "a DataUpdater process is not duplicated when using the same sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = DataUpdater.get_data_updater_process(sharding_key)

    dup_processes = Registry.select(Registry.DataUpdater, [{{:"$1", :_, :_}, [], [:"$1"]}])
    assert [{:sharding_key, ^sharding_key}] = dup_processes

    {:ok, ^pid} = DataUpdater.get_data_updater_process(sharding_key)
    assert [{:sharding_key, ^sharding_key}] = dup_processes

    on_exit(fn -> DataUpdater.Supervisor.terminate_child(pid) end)
  end

  test "a DataUpdater process is spawned again if requested after termination" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, first_pid} = DataUpdater.get_data_updater_process(sharding_key)

    du_process =
      Registry.select(Registry.DataUpdater, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    assert [{{:sharding_key, ^sharding_key}, ^first_pid}] = du_process

    DynamicSupervisor.terminate_child(DataUpdater.Supervisor, first_pid)
    # Give some time to the Registry to delete the entry
    Process.sleep(:timer.seconds(2))
    [] = Registry.select(Registry.DataUpdater, [{{:"$1", :_, :_}, [], [:"$1"]}])

    {:ok, second_pid} = DataUpdater.get_data_updater_process(sharding_key)
    assert first_pid != second_pid

    du_process =
      Registry.select(Registry.DataUpdater, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    assert [{{:sharding_key, ^sharding_key}, ^second_pid}] = du_process

    on_exit(fn -> DynamicSupervisor.terminate_child(DataUpdater.Supervisor, second_pid) end)
  end
end
