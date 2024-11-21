defmodule Mississippi.Consumer.Options do
  @moduledoc false

  alias Mississippi.Consumer.ConsumersSupervisor

  definition =
    [
      amqp_consumer_options: [
        type: :keyword_list,
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
          ],
          channels: [
            type: :pos_integer,
            default: 10,
            doc: """
            The number of AMQP channels to open for each AMQP connection.
            """
          ]
        ]
      ],
      cluster_topologies: [
        type: :keyword_list,
        doc: """
        The libcluster topologies to form an Erlang cluster.
        See https://hexdocs.pm/libcluster/readme.html#clustering.
        """,
        default: []
      ]
    ] ++
      ConsumersSupervisor.init_opts()

  @definition NimbleOptions.new!(definition)

  def definition, do: @definition
end
