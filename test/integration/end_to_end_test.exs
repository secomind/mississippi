# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.EndToEnd.Test do
  use ExUnit.Case

  alias Mississippi.Producer.EventsProducer

  require Logger

  @moduletag :integration

  setup_all do
    queue_count = :rand.uniform(20)

    prefix = "mississippi_test_#{System.unique_integer()}_"
    exchange_name = "mississippi_#{System.unique_integer([:positive])}"

    producer_options = [
      amqp_producer_options: [host: "localhost"],
      mississippi_config: [
        queues: [events_exchange_name: exchange_name, total_count: queue_count, prefix: prefix]
      ]
    ]

    consumer_options = [
      amqp_consumer_options: [host: "localhost"],
      mississippi_config: [
        queues: [
          events_exchange_name: exchange_name,
          prefix: prefix,
          range_start: 0,
          range_end: queue_count - 1,
          total_count: queue_count
        ],
        message_handler: E2EMessageHandler
      ]
    ]

    consumer = start_supervised!({Mississippi.Consumer, consumer_options})
    producer = start_supervised!({Mississippi.Producer, producer_options})

    %{
      producer: producer,
      consumer: consumer
    }
  end

  setup do
    E2EMessageHandler.start_with_receiver(self())
    Process.sleep(500)

    %{
      sharding_key: "sharding_key_#{System.unique_integer()}",
      payload: "payload_#{System.unique_integer()}",
      timestamp: DateTime.to_unix(DateTime.utc_now())
    }
  end

  @tag :e2e
  test "Message is published and received", %{
    sharding_key: sharding_key,
    payload: payload,
    timestamp: timestamp
  } do
    EventsProducer.publish(payload, sharding_key: sharding_key)

    assert_receive {^payload, headers, ^timestamp}
    assert :erlang.binary_to_term(headers["sharding_key"]) == sharding_key
  end
end
