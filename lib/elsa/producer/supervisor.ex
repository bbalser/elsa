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
    name = Keyword.fetch!(args, :name)
    Supervisor.start_link(__MODULE__, args, name: name)
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
        name = :"producer_#{topic}_#{partition}"
        child_spec(brod_client, topic, partition, config, {registry, name})
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp child_spec(brod_client, topic, partition, config, registration) do
    wrapper_args = [
      mfa: {:brod_producer, :start_link, [brod_client, topic, partition, config]},
      register: registration
    ]

    %{
      id: :"#{topic}-#{partition}",
      start: {Elsa.Wrapper, :start_link, [wrapper_args]}
    }
  end
end
