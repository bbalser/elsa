defmodule Elsa.Consumer.Worker.Initializer do
  @type init_opts :: [
          connection: atom(),
          registry: atom(),
          topics: [Elsa.topic() | {Elsa.topic(), Elsa.partition()}]
        ]

  @spec init(init_opts) :: [Supervisor.child_spec()]
  def init(init_arg) do
    registry = Keyword.fetch!(init_arg, :registry)
    topics = Keyword.fetch!(init_arg, :topics)

    brod_client = Elsa.Registry.whereis_name({registry, :brod_client})

    Enum.map(topics, &configure_topic(&1, registry, brod_client, init_arg))
    |> List.flatten()
  end

  defp configure_topic({topic, partition}, registry, _brod_client, init_arg) do
    child_spec(registry, topic, partition, init_arg)
    |> List.wrap()
  end

  defp configure_topic(topic, registry, brod_client, init_arg) do
    Elsa.Util.partition_count(brod_client, topic)
    |> to_child_specs(registry, topic, init_arg)
  end

  defp to_child_specs(partitions, registry, topic, init_arg) do
    0..(partitions - 1)
    |> Enum.map(fn partition ->
      child_spec(registry, topic, partition, init_arg)
    end)
  end

  defp child_spec(registry, topic, partition, init_arg) do
    name = :"topic_consumer_worker_#{topic}_#{partition}"

    {Elsa.Consumer.Worker,
     init_arg
     |> Keyword.put(:name, {:via, Elsa.Registry, {registry, name}})
     |> Keyword.put(:topic, topic)
     |> Keyword.put(:partition, partition)}
    |> Supervisor.child_spec(id: name)
  end
end
