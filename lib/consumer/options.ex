# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.Options do
  @moduledoc false

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
      mississippi_config: [
        type: :keyword_list,
        default: [],
        keys: [
          queues: [
            type: :keyword_list,
            default: [],
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
                doc: """
                The start index of the range of queues that this Mississippi consumer instance will handle.
                This option is deprecated and will be ignored.
                """
              ],
              range_end: [
                type: :non_neg_integer,
                doc: """
                The end index of the range of queues that this Mississippi consumer instance will handle.
                This option is deprecated and will be ignored.
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
          ],
          cluster_distribution_strategy: [
            type: {:in, [:uniform_quorum, :uniform_random, :uniform]},
            default: :uniform_quorum,
            doc: """
            The strategy to use for redistributing consumer processes within the cluster.
            """
          ]
        ]
      ]
    ]

  @definition NimbleOptions.new!(definition)

  def definition, do: @definition
end
