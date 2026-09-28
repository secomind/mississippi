# Copyright 2026 Clea Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.Healthcheck do
  @moduledoc """
  Healthcheck functions for the EventsProducer supervision tree.
  """

  alias Horde.DynamicSupervisor
  alias Horde.Registry
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.Worker

  require Logger

  @doc """
  Runs a list of healthchecks. Possible errors returned:
  - :workers_count_mismatch -> the number of running EventProducer workers does not match
  the number of configured/expected workers
  - :events_producer_uninitialized -> the EventsProducer is not initialized
  - :no_active_workers -> all workers are DOWN (possibly restarting all at once)
  - :amqp_channels_down -> AMQP channel is not established for some of the workers
  """
  @spec check_all() :: :ok | {:error, [atom()]}
  def check_all do
    active_workers = DynamicSupervisor.which_children(Worker.Supervisor)
    active_workers_count = Enum.count(active_workers)

    count_error =
      case Registry.lookup(EventsProducer.Registry, :events_producer_config) do
        [{_pid, queues_config}] ->
          expected_workers_count = Keyword.fetch!(queues_config, :total_count)

          if active_workers_count != expected_workers_count do
            Logger.warning(
              "Currently active EventsProducer workers: #{active_workers_count}, expected: #{expected_workers_count}"
            )

            :workers_count_mismatch
          else
            nil
          end

        [] ->
          :events_producer_uninitialized
      end

    channel_error =
      cond do
        active_workers_count == 0 ->
          # if this error is reported, it may be that all the workers
          # are restarting since the AMQP connection went down
          :no_active_workers

        not all_channels_up?(active_workers) ->
          :amqp_channels_down

        true ->
          nil
      end

    error_list = Enum.reject([count_error, channel_error], &is_nil/1)

    if error_list == [], do: :ok, else: {:error, error_list}
  end

  defp all_channels_up?(workers) do
    Enum.all?(workers, fn {_, pid, :worker, _} ->
      Worker.get_amqp_channel_status(pid) == :up
    end)
  end
end
