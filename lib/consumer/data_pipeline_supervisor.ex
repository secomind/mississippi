defmodule Mississippi.Consumer.DataPipelineSupervisor do
  use Supervisor

  alias Mississippi.Consumer.ConsumersSupervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    amqp_consumer_options = Keyword.fetch!(init_arg, :amqp_consumer_options)

    events_consumer_connection_number =
      Keyword.fetch!(init_arg, :events_consumer_connection_number)

    events_producer_config = Keyword.fetch!(init_arg, :mississippi_queues_config)

    message_handler =
      Keyword.get(init_arg, :message_handler, Mississippi.Consumer.DataUpdater.Handler.Impl)

    children = [
      {Registry, [keys: :unique, name: Registry.MessageTracker]},
      {Registry, [keys: :unique, name: Registry.DataUpdater]},
      {ExRabbitPool.PoolSupervisor,
       rabbitmq_config: amqp_consumer_options,
       connection_pools: [events_consumer_pool_config(events_consumer_connection_number)]},
      {ConsumersSupervisor,
       mississippi_queues_config: events_producer_config, message_handler: message_handler}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp events_consumer_pool_config(events_consumer_connection_number) do
    [
      name: {:local, :events_consumer_pool},
      worker_module: ExRabbitPool.Worker.RabbitConnection,
      size: events_consumer_connection_number,
      max_overflow: 0
    ]
  end
end
