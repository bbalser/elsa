defmodule Elsa.Producer.Initializer do
  @spec init(registry :: atom(), producer_configs :: list(keyword)) :: [Supervisor.child_spec()]
  def init(registry, producer_configs) do
    brod_client = Elsa.Registry.whereis_name({registry, :brod_client})

    case Keyword.keyword?(producer_configs) do
      true ->
        child_spec(registry, brod_client, producer_configs)

      false ->
        Enum.map(producer_configs, fn pc ->
          child_spec(registry, brod_client, pc)
        end)
        |> List.flatten()
    end
  end

  defp child_spec(registry, brod_client, producer_config) do
    topic = Keyword.fetch!(producer_config, :topic)
    config = Keyword.get(producer_config, :config, [])

    partitions = Elsa.Util.partition_count(brod_client, topic)

    0..(partitions - 1)
    |> Enum.map(fn partition ->
      child_spec(registry, brod_client, topic, partition, config)
    end)
  end

  defp child_spec(registry, brod_client, topic, partition, config) do
    name = :"producer_#{topic}_#{partition}"

    wrapper_args = [
      mfa: {:brod_producer, :start_link, [brod_client, topic, partition, config]},
      register: {registry, name}
    ]

    %{
      id: name,
      start: {Elsa.Wrapper, :start_link, [wrapper_args]}
    }
  end
end
