defmodule Elsa.Group.ConsumerSupervisor do
  use Supervisor

  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    topic = Keyword.fetch!(args, :topic)
    Supervisor.start_link(__MODULE__, args, name: {:via, Elsa.Registry, {registry, :"consumer_supervisor_#{topic}"}})
  end

  def init(args) do
    registry = Keyword.fetch!(args, :registry)
    topic = Keyword.fetch!(args, :topic)
    config = Keyword.fetch!(args, :config)

    brod_client = Elsa.Registry.whereis_name({registry, :brod_client})
    {:ok, partitions} = :brod_client.get_partitions_count(brod_client, topic)

    children =
      0..(partitions - 1)
      |> Enum.map(fn partition ->
        child_spec(registry, brod_client, topic, partition, config)
      end)

    Supervisor.init(children, strategy: :one_for_one)
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