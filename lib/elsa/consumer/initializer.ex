defmodule Elsa.Consumer.Initializer do
  @type init_opts :: [
          registry: atom(),
          topics: [Elsa.topic() | {Elsa.topic(), Elsa.partition()}],
          config: :brod_consumer.config()
        ]

  @spec init(init_opts) :: [Supervisor.child_spec()]
  def init(init_arg) do
    registry = Keyword.fetch!(init_arg, :registry)
    topics = Keyword.fetch!(init_arg, :topics)
    config = Keyword.fetch!(init_arg, :config)

    brod_client = Elsa.Registry.whereis_name({registry, :brod_client})

    Enum.map(topics, &configure_topic(&1, registry, brod_client, config))
    |> List.flatten()
  end

  defp configure_topic({topic, partition}, registry, brod_client, config) do
    child_spec(registry, brod_client, topic, partition, config)
    |> List.wrap()
  end

  defp configure_topic(topic, registry, brod_client, config) do
    :brod_client.get_metadata(brod_client, topic)
    :brod_client.get_partitions_count(brod_client, topic)
    |> to_child_specs(registry, brod_client, topic, config)
  end

  defp to_child_specs({:ok, partitions}, registry, brod_client, topic, config) do
    0..(partitions - 1)
    |> Enum.map(fn partition ->
      child_spec(registry, brod_client, topic, partition, config)
    end)
  end

  defp child_spec(registry, brod_client, topic, partition, config) do
    name = :"consumer_#{topic}_#{partition}"

    wrapper_args = [
      mfa: {:brod_consumer, :start_link, [brod_client, topic, partition, config]},
      register: {registry, name}
    ]

    %{
      id: name,
      start: {Elsa.Wrapper, :start_link, [wrapper_args]}
    }
  end
end
