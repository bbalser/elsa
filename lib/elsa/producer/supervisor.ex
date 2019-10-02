defmodule Elsa.Producer.Supervisor do
  @moduledoc """
  Supervisor that starts and manages brod producer processes,
  one per topic/partition by way of the Elsa Wrapper GenServer.
  """
  use Supervisor

  @doc """
  Start the producer supervisor process and link it to the current process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    topic = Keyword.fetch!(args, :topic)
    Supervisor.start_link(__MODULE__, args, name: {:via, Elsa.Registry, {registry, :"producer_supervisor_#{topic}"}})
  end

  def child_spec(args) do
    topic = Keyword.fetch!(args, :topic)
    Supervisor.child_spec(super(args), id: :"producer_supervisor_#{topic}")
  end

  @doc """
  On startup, retrieves the number of partitions for the given topic
  and constructs a child spec definition for an Elsa Wrapper process
  to start and link to a brod producer for each one.
  """
  def init(opts) do
    registry = Keyword.fetch!(opts, :registry)
    topic = Keyword.fetch!(opts, :topic)
    config = Keyword.get(opts, :config, [])

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
