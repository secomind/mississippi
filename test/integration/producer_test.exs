# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Integration.Producer.Test do
  use ExUnit.Case

  alias Mississippi.Producer.EventsProducer

  require Logger

  @moduletag :integration

  setup_all do
    queue_count = System.unique_integer([:positive])
    prefix = "mississippi_test_#{System.unique_integer()}_"
    exchange_name = "mississippi_#{System.unique_integer([:positive])}"

    producer_options = [
      amqp_producer_options: [host: "localhost"],
      mississippi_config: [
        queues: [events_exchange_name: exchange_name, total_count: queue_count, prefix: prefix]
      ]
    ]

    %{
      producer: start_supervised!({Mississippi.Producer, producer_options}),
      exchange_name: exchange_name,
      queue_count: queue_count,
      queue_prefix: prefix
    }
  end

  describe "Message sharding:" do
    setup :create_sharding_key
    setup :create_payload
    setup :setup_message_handler
    setup :setup_amqp_consumer

    @tag :producer_message_sharding
    test "Message is published on the correct queue according to the sharding key", %{
      sharding_key: sharding_key,
      payload: payload
    } do
      EventsProducer.publish(payload, sharding_key: sharding_key)

      assert_receive {^payload, headers, _timestamp}
      assert :erlang.binary_to_term(headers["sharding_key"]) == sharding_key
    end
  end

  describe "Message options:" do
    setup :create_sharding_key
    setup :create_payload
    setup :setup_amqp_consumer

    @tag :producer_message_options
    test "Timestamp is added to the message if missing", %{
      sharding_key: sharding_key,
      payload: payload
    } do
      EventsProducer.publish(payload, sharding_key: sharding_key)

      assert_receive {^payload, _headers, timestamp}
      assert timestamp
    end

    @tag :producer_message_options
    test "Timestamp is correctly included in the message if present", %{
      sharding_key: sharding_key,
      payload: payload
    } do
      timestamp = DateTime.to_unix(DateTime.utc_now())
      EventsProducer.publish(payload, sharding_key: sharding_key, timestamp: timestamp)

      assert_receive {^payload, _headers, ^timestamp}
    end
  end

  defp create_sharding_key(context) do
    sharding_key = "sharding_key_#{System.unique_integer()}"
    Map.put(context, :sharding_key, sharding_key)
  end

  defp create_payload(context) do
    payload = "payload_#{System.unique_integer()}"
    Map.put(context, :payload, payload)
  end

  defp get_amqp_consumer_for(exchange_name, queue_name) do
    pid =
      start_link_supervised!({
        SimpleAMQPConsumer,
        [
          exchange: exchange_name,
          queue_name: queue_name,
          receiver: self(),
          consumer_options: [host: "localhost"]
        ]
      })

    barrier = SimpleAMQPConsumer.consumer_ready_message(exchange_name, queue_name)

    # wait for consumer to be ready
    assert_receive ^barrier

    pid
  end

  defp setup_amqp_consumer(context) do
    %{
      queue_count: queue_count,
      queue_prefix: queue_prefix,
      sharding_key: sharding_key,
      exchange_name: exchange_name
    } = context

    queue_index = :erlang.phash2(sharding_key, queue_count)
    queue_name = "#{queue_prefix}#{queue_index}"

    consumer = get_amqp_consumer_for(exchange_name, queue_name)
    %{queue_index: queue_index, queue_name: queue_name, consumer: consumer}
  end

  defp setup_message_handler(_context) do
    E2EMessageHandler.start_with_receiver(self())

    :ok
  end
end
