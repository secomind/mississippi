defmodule Mississippi.Consumer.ConsumersSupervisor do
  use Supervisor
  require Logger

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    events_producer_config = Keyword.fetch!(init_arg, :mississippi_queues_config)
    message_handler = Keyword.fetch!(init_arg, :message_handler)

    Logger.info("AMQPDataConsumer supervisor init.", tag: "data_consumer_sup_init")

    children = [
      {Registry, [keys: :unique, name: Registry.AMQPDataConsumer]},
      {Mississippi.Consumer.AMQPDataConsumer.Supervisor,
       mississippi_queues_config: events_producer_config, message_handler: message_handler}
    ]

    opts = [strategy: :rest_for_one, name: __MODULE__]

    Supervisor.init(children, opts)
  end
end
