# Mississippi

Distributed message processing framework over AMQP.
Messages are sharded into a configurable amount of queues,
and the ones belonging to the same shard are guaranteed to be processed
in the same order as they were sent. The sharding key can be any Erlang term!
There can be any number of consumers, as long as they process messages from
non-overlapping queue ranges.

## Try it!
First of all, let's bring up a RabbitMQ instance:

```sh
docker run --rm -p 5672:5672 -p 15672:15672 rabbitmq:3.8.34-management
```
### Producer
Start a producer with
```elixir
init_options = [
    amqp_producer_options: [host: "localhost"],
    mississippi_config: [
        queues: [events_exchange_name: "", total_count: 128, prefix: "mississippi_"]
    ]
]
# [...]
Mississippi.Producer.start_link(init_options)
# {:ok, <pid>}
```
The producer will publish data on 128 AMQP queues (0 to 127).
To do so:
```elixir
Mississippi.Producer.EventsProducer.publish("aaa", sharding_key: "user_1")
# :ok
```

### Consumer
Start a consumer with
```elixir
init_options = [
    amqp_consumer_options: [host: "localhost"],
    mississippi_config: [
        queues: [events_exchange_name: "", prefix: "mississippi_", range_start: 0, range_end: 127, total_count: 128],
        message_handler: My.Custom.MessageHandler
    ]
]
# [...]
Mississippi.Consumer.start_link(init_options)
# {:ok, <pid>}
```

The message handler can be customized according to your needs: the
`Mississippi.Consumer.DataUpdater.Handler` behaviour is provided to do so.
A default implementation that just prints the message to standard output is available
at `lib/consumer/data_updater/impl.ex`.


# License

Mississippi source code is released under the Apache 2 License.

Check the LICENSE file for more information.
