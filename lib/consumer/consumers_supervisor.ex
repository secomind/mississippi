defmodule Mississippi.Consumer.ConsumersSupervisor do
  use Supervisor
  require Logger

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    Logger.info("AMQPDataConsumer supervisor init.", tag: "data_consumer_sup_init")

    children = [
      {Registry, [keys: :unique, name: Registry.AMQPDataConsumer]},
      {Mississippi.Consumer.AMQPDataConsumer.Supervisor, init_arg}
    ]

    opts = [strategy: :rest_for_one, name: __MODULE__]

    Supervisor.init(children, opts)
  end

  @doc false
  def init_opts() do
    [
      mississippi_config: [
        type: :keyword_list,
        keys: [
          queues: [
            type: :keyword_list,
            keys: [
              events_exchange_name: [
                type: :string,
                default: "",
                doc: """
                The name of the exchange on which Mississippi messages will be published.
                Must be the same as the one used by the consumer.
                """
              ],
              total_count: [
                type: :pos_integer,
                default: 128,
                doc: """
                The number of queues on which Mississippi messages will be sharded.
                Must be the same as the one used by the producer.
                """
              ],
              range_start: [
                type: :non_neg_integer,
                default: 0,
                doc: """
                The start index of the range of queues that this Mississippi consumer instance will handle.
                """
              ],
              range_end: [
                type: :non_neg_integer,
                default: 127,
                doc: """
                The end index of the range of queues that this Mississippi consumer instance will handle.
                """
              ],
              prefix: [
                type: :string,
                default: "mississippi_",
                doc: """
                A string prefix for naming the queues on which Mississippi messages
                will be sharded. Must be the same as the one used by the consumer.
                """
              ]
            ]
          ],
          message_handler: [
            type: :atom,
            default: Mississippi.Consumer.DataUpdater.Handler.Impl,
            doc: """
            The module that will be invoked by Mississippi to process incoming messages.
            It must implement the `Mississippi.Consumer.DataUpdater.Handler` behaviour.
            """
          ]
        ]
      ]
    ]
  end
end
