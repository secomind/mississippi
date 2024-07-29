defmodule Mississippi.Integration.Producer.Test do
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

    start_consumer_pool!()

    %{
      producer: start_supervised!({Mississippi.Producer, producer_options}),
      queue_count: queue_count,
      queue_prefix: prefix
    }
  end

  describe "Message sharding:" do
    setup :create_sharding_key
    setup :create_payload
    setup :setup_message_handler

    @tag :producer_message_sharding
    test "Message is published on the correct queue according to the sharding key", %{
      sharding_key: sharding_key,
      payload: payload,
      queue_count: queue_count,
      queue_prefix: queue_prefix
    } do
      expected_queue_index = :erlang.phash2(sharding_key, queue_count)
      expected_queue_name = "#{queue_prefix}#{expected_queue_index}"

      _ = get_amqp_consumer_for(expected_queue_name)

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

  defp start_consumer_pool! do
    start_supervised!(
      {ExRabbitPool.PoolSupervisor,
       rabbitmq_config: [host: "localhost"],
       connection_pools: [
         [
           name: {:local, :events_consumer_pool},
           worker_module: ExRabbitPool.Worker.RabbitConnection,
           size: 1,
           max_overflow: 0
         ]
       ]}
    )
  end

  defp create_sharding_key(context) do
    sharding_key = "sharding_key_#{System.unique_integer()}"
    Map.put(context, :sharding_key, sharding_key)
  end

  defp create_payload(context) do
    payload = "payload_#{System.unique_integer()}"
    Map.put(context, :payload, payload)
  end

  defp get_amqp_consumer_for(queue_name) do
    GenServer.start_link(SimpleAMQPConsumer, queue_name: queue_name, receiver: self())
  end

  defp setup_amqp_consumer(context) do
    %{queue_count: queue_count, queue_prefix: queue_prefix, sharding_key: sharding_key} = context
    queue_index = :erlang.phash2(sharding_key, queue_count)
    queue_name = "#{queue_prefix}#{queue_index}"

    get_amqp_consumer_for(queue_name)
    context
  end

  defp setup_message_handler(context) do
    E2EMessageHandler.start_with_receiver(self())

    context
  end
end
