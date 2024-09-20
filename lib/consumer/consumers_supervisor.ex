defmodule Mississippi.Consumer.ConsumersSupervisor do
  @moduledoc false
  use Supervisor

  alias Horde.DynamicSupervisor
  alias Horde.Registry
  alias Mississippi.Consumer.AMQPDataConsumer
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.MessageTracker

  require Logger

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    Logger.info("ConsumersSupervisor init.")

    message_handler = init_arg[:message_handler]

    queues_config = init_arg[:queues]

    children = [
      {Cluster.Supervisor, [cluster_topologies(), [name: Mississippi.Consumer.ClusterSupervisor]]},
      {Registry, [keys: :unique, name: DataUpdater.Registry, members: :auto]},
      {Registry, [keys: :unique, name: MessageTracker.Registry, members: :auto]},
      {Registry, [keys: :unique, name: AMQPDataConsumer.Registry, members: :auto]},
      {DataUpdater.Supervisor, message_handler: message_handler},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: MessageTracker.Supervisor,
       members: :auto,
       process_redistribution: :active,
       distribution_strategy: Horde.UniformQuorumDistribution},
      {AMQPDataConsumer.Supervisor, queues_config: queues_config},
      # This will make queues start after re-sharding in a multi-node cluster
      {NodeListener, queues_config},
      # This will make queues start in a single-node cluster
      {Task, fn -> AMQPDataConsumer.Supervisor.start_consumers(queues_config) end}
    ]

    opts = [strategy: :rest_for_one]

    Supervisor.init(children, opts)
  end

  # TODO find out a suitable set of topologies
  defp cluster_topologies do
    []
  end

  @doc false
  def init_opts do
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
          ]
        ]
      ]
    ]
  end
end
