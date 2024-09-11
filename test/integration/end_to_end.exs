# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.EndToEnd.Test do
  use ExUnit.Case

  import Hammox

  alias Mississippi.Consumer.AMQPDataConsumer.ExRabbitPoolConnection
  alias Mississippi.Producer.EventsProducer

  require Logger

  @moduletag :integration

  setup_all do
    stub_with(MockAMQPConnection, ExRabbitPoolConnection)
    Hammox.set_mox_global()
    queue_count = System.unique_integer([:positive])

    prefix = "mississippi_test_#{System.unique_integer()}_"
    # We use the default exchange so that queues are binded using the routing key
    exchange_name = ""

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

    %{
      producer: start_supervised!({Mississippi.Producer, producer_options}),
      consumer: start_supervised!({Mississippi.Consumer, consumer_options})
    }
  end

  setup do
    E2EMessageHandler.start_with_receiver(self())

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
