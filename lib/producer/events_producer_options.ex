defmodule Mississippi.Producer.EventsProducer.Options do
  @moduledoc false

  def producer_opts do
    [
      type: :keyword_list,
      required: true,
      keys: [
        events_exchange_name: [
          type: :string,
          default: "",
          doc: """
          The name of the exchange on which Mississippi messages will be published.
          Must be the same as the one used by the consumer.
          """
        ],
        data_queue_count: [
          type: :pos_integer,
          default: 128,
          doc: """
          The number of queues on which Mississippi messages will be sharded.
          Must be the same as the one used by the consumer.
          """
        ],
        data_queue_prefix: [
          type: :string,
          default: "mississippi_",
          doc: """
          A string prefix for naming the queues on which Mississippi messages
          will be sharded. Must be the same as the one used by the consumer.
          """
        ]
      ]
    ]
  end

  publish_opts = [
    sharding_key: [
      type: :any,
      required: true,
      doc: """
      The key according to which data will be sharded among Mississipi queues.
      Ordering is guaranteed only between data sharing the same sharding_key.
      """
    ],
    headers: [
      type: {:or, [:keyword_list, :map]},
      doc: """
      Optional additional headers to be included in the message.
      """
    ]
  ]

  @publish_opts NimbleOptions.new!(publish_opts)

  def publish_opts, do: @publish_opts
end
