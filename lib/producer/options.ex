defmodule Mississippi.Producer.Options do
  @moduledoc false

  alias Mississippi.Producer.EventsProducer

  definition = [
    amqp_producer_options: [
      type: :keyword_list,
      required: true,
      keys: [
        username: [
          type: :string,
          default: "guest"
        ],
        password: [
          type: :string,
          default: "guest"
        ],
        virtual_host: [
          type: :string,
          default: "/"
        ],
        host: [
          type: :string,
          default: "localhost"
        ],
        port: [
          type: :pos_integer,
          default: 5672
        ],
        ssl_options: [
          type: :keyword_list
        ]
      ]
    ],
    events_producer_connection_number: [
      type: :pos_integer,
      required: true
    ],
    mississippi_config: EventsProducer.Options.producer_opts()
  ]

  @definition NimbleOptions.new!(definition)

  def definition, do: @definition
end
