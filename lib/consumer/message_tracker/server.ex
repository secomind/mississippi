defmodule Mississippi.Consumer.MessageTracker.Server do
  alias Mississippi.Consumer.AMQPDataConsumer
  require Logger
  use GenServer

  @base_backoff 1000
  @random_backoff 9000

  # TODO: this should probably be a :gen_statem so we can simplify state data

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    _ = Logger.info("Starting MessageTracker #{inspect(name)}", tag: "message_tracker_start")
    GenServer.start_link(__MODULE__, args, name: name)
  end

  def init(init_args) do
    acknowledger = Keyword.fetch!(init_args, :acknowledger)
    {:ok, {:new, :queue.new(), %{}, acknowledger}}
  end

  def handle_call(:register_data_updater, from, {:new, queue, ids, acknowledger}) do
    monitor(from)
    {:reply, :ok, {:accepting, queue, ids, acknowledger}}
  end

  def handle_call(:register_data_updater, from, {_state, queue, ids, acknowledger}) do
    Logger.debug("Blocked data updater registration. Queue is #{inspect(queue)}.")

    {:noreply, {{:waiting_cleanup, from}, queue, ids, acknowledger}}
  end

  def handle_call(
        {:can_process_message, message_id},
        from,
        {:accepting, queue, ids, acknowledger} = s
      ) do
    case :queue.peek(queue) do
      {:value, ^message_id} ->
        case Map.get(ids, message_id) do
          nil ->
            {:noreply, {{:waiting_delivery, from}, queue, ids, acknowledger}}

          {:requeued, _delivery_tag} ->
            {:noreply, {{:waiting_delivery, from}, queue, ids, acknowledger}}

          _ ->
            {:reply, true, s}
        end

      {:value, _} ->
        {:reply, false, s}

      :empty ->
        Logger.debug("#{inspect(message_id)} has not been tracked yet. Waiting.")
        {:noreply, {{:waiting_delivery, message_id, from}, queue, ids, acknowledger}}
    end
  end

  def handle_call({:ack_delivery, message_id}, _from, {:accepting, queue, ids, acknowledger}) do
    {{:value, ^message_id}, new_queue} = :queue.out(queue)
    {delivery_tag, new_ids} = Map.pop(ids, message_id)

    :ok = ack(acknowledger, delivery_tag)

    {:reply, :ok, {:accepting, new_queue, new_ids, acknowledger}}
  end

  def handle_call({:discard, message_id}, _from, {:accepting, queue, ids, acknowledger}) do
    {{:value, ^message_id}, new_queue} = :queue.out(queue)
    {delivery_tag, new_ids} = Map.pop(ids, message_id)

    :ok = discard(acknowledger, delivery_tag)

    {:reply, :ok, {:accepting, new_queue, new_ids, acknowledger}}
  end

  def handle_call(:deactivate, _from, {state, queue, ids, _acknowledger} = s) do
    cond do
      not :queue.is_empty(queue) ->
        # We are in a dirty state, so we will not deactivate and we return an error
        Logger.warning("Refusing to deactivate MessageTracker with non-empty queue.",
          tag: "message_tracker_deactivate_failed"
        )

        {:reply, {:error, :deactivate_failed}, s}

      ids != %{} ->
        # We are in a dirty state, so we will not deactivate and we return an error
        Logger.warning("Refusing to deactivate MessageTracker with non-empty ids.",
          tag: "message_tracker_deactivate_failed"
        )

        {:reply, {:error, :deactivate_failed}, s}

      state != :accepting ->
        # We are in a dirty state, so we will not deactivate and we return an error
        Logger.warning("Refusing to deactivate MessageTracker not in :accepting state.",
          tag: "message_tracker_deactivate_failed"
        )

        {:reply, {:error, :deactivate_failed}, s}

      true ->
        # Everything is clean, we can deactivate
        {:stop, :normal, :ok, s}
    end
  end

  def handle_cast(
        {:track_delivery, message_id, delivery_tag},
        {{:waiting_delivery, waiting_process}, queue, ids, acknowledger}
      ) do
    case Map.get(ids, message_id) do
      nil ->
        {new_queue, new_ids} = enqueue_message(queue, ids, message_id, delivery_tag)
        {:noreply, {{:waiting_delivery, waiting_process}, new_queue, new_ids, acknowledger}}

      {:requeued, _tag} ->
        new_ids = Map.put(ids, message_id, delivery_tag)

        if :queue.peek(queue) == {:value, message_id} do
          GenServer.reply(waiting_process, true)
          {:noreply, {:accepting, queue, new_ids, acknowledger}}
        else
          {:noreply, {{:waiting_delivery, waiting_process}, queue, new_ids, acknowledger}}
        end

      _ ->
        new_ids = Map.put(ids, message_id, delivery_tag)
        {:noreply, {{:waiting_delivery, waiting_process}, queue, new_ids, acknowledger}}
    end
  end

  def handle_cast(
        {:track_delivery, message_id, delivery_tag},
        {state, queue, ids, acknowledger}
      ) do
    unless Map.has_key?(ids, message_id) do
      {new_queue, new_ids} = enqueue_message(queue, ids, message_id, delivery_tag)
      {:noreply, {state, new_queue, new_ids, acknowledger}}
    else
      new_ids = Map.put(ids, message_id, delivery_tag)
      {:noreply, {state, queue, new_ids, acknowledger}}
    end
  end

  def handle_info(
        {:DOWN, _, :process, _pid, reason},
        {state, queue, ids, acknowledger} = s
      ) do
    Logger.warning("Crash detected. Reason: #{inspect(reason)}, state: #{inspect(s)}.",
      tag: "data_upd_crash_detected"
    )

    # TODO
    # :telemetry.execute([:astarte, :data_updater_plant, :data_updater, :detected_crash], %{}, %{})

    marked_ids =
      :queue.to_list(queue)
      |> List.foldl(%{}, fn item, acc ->
        delivery_tag = ids[item]
        :ok = requeue(acknowledger, delivery_tag)
        Map.put(acc, item, {:requeued, delivery_tag})
      end)

    unless :queue.is_empty(queue) do
      :rand.uniform(@random_backoff)
      |> Kernel.+(@base_backoff)
      |> :timer.sleep()
    end

    case state do
      {:waiting_cleanup, waiting_process} ->
        monitor(waiting_process)
        GenServer.reply(waiting_process, :ok)
        {:noreply, {:accepting, queue, marked_ids, acknowledger}}

      _ ->
        {:noreply, {:new, queue, marked_ids, acknowledger}}
    end
  end

  defp monitor({pid, _ref}) do
    Process.monitor(pid)
  end

  defp enqueue_message(queue, ids, message_id, delivery_tag) do
    new_ids = Map.put(ids, message_id, delivery_tag)
    new_queue = :queue.in(message_id, queue)
    {new_queue, new_ids}
  end

  defp requeue(_acknowledger, {:injected_msg, _ref}) do
    :ok
  end

  defp requeue(acknowledger, delivery_tag) when is_integer(delivery_tag) do
    AMQPDataConsumer.requeue(acknowledger, delivery_tag)
  end

  defp requeue(_acknowledger, {:requeued, delivery_tag}) when is_integer(delivery_tag) do
    # Do not try to requeue already requeued messages, otherwise channel will crash
    :ok
  end

  defp ack(_acknowledger, {:injected_msg, _ref}) do
    :ok
  end

  defp ack(acknowledger, delivery_tag) when is_integer(delivery_tag) do
    AMQPDataConsumer.ack(acknowledger, delivery_tag)
  end

  defp discard(_acknowledger, {:injected_msg, _ref}) do
    :ok
  end

  defp discard(acknowledger, delivery_tag) when is_integer(delivery_tag) do
    AMQPDataConsumer.discard(acknowledger, delivery_tag)
  end
end
