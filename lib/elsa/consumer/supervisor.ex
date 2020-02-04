defmodule Elsa.Consumer.Supervisor do
  @moduledoc """
  Supervisor that starts and manages brod consumer processes,
  one per topic/partition by way of the Elsa Wrapper GenServer.
  """
  use Supervisor

  @type init_opts :: [
    registry: atom,
    topic: Elsa.topic(),
    partition: Elsa.partition(),
    config: :brod_consumer.config()
  ]

  @doc """
  Start the consumer supervisor process and link it to the current process.
  Registers itself to the Elsa Registry.
  """
  @spec start_link(init_opts) :: GenServer.on_start()
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    topic = Keyword.fetch!(args, :topic)

    Supervisor.start_link(__MODULE__, args, name: {:via, Elsa.Registry, {registry, :"consumer_supervisor_#{topic}"}})
  end

  @doc """
  On startup, retrieves the number of partitions for the given topic
  and constructs a child spec definition for an Elsa Wrapper process
  to start and link to a brod consumer for each one.
  """
  def init(args) do
    registry = Keyword.fetch!(args, :registry)
    topic = Keyword.fetch!(args, :topic)
    config = Keyword.fetch!(args, :config)

    brod_client = Elsa.Registry.whereis_name({registry, :brod_client})

    Keyword.get_lazy(args, :partition, fn ->
      :brod_client.get_partitions_count(brod_client, topic)
    end)
    |> to_child_specs(registry, brod_client, topic, config)
    |> Supervisor.init(strategy: :one_for_one)
  end

  defp to_child_specs({:ok, partitions}, registry, brod_client, topic, config) do
    0..(partitions - 1)
    |> Enum.map(fn partition ->
      child_spec(registry, brod_client, topic, partition, config)
    end)
  end

  defp to_child_specs(partition, registry, brod_client, topic, config)
       when is_integer(partition) do
    child_spec(registry, brod_client, topic, partition, config)
    |> List.wrap()
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
