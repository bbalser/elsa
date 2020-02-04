defmodule Elsa.Consumer.WorkerSupervisor do
  @moduledoc """
  Supervisor that starts and manages consumer worker processes based on
  given configuration. Without a specified `:partition`, starts a worker for
  each partition on the configured topic. Otherwise, starts a worker for the
  single, specified partition.
  """
  use Supervisor

  @type init_opts :: [
    registry: atom,
    topic: Elsa.topic(),
    partition: Elsa.partition(),
  ]

  @doc """
  Start the consumer worker supervisor and link it to the current process.
  Registers itself to the Elsa Registry.
  """
  @spec start_link(init_opts) :: GenServer.on_start()
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    topic = Keyword.fetch!(args, :topic)

    Supervisor.start_link(__MODULE__, args,
      name: {:via, Elsa.Registry, {registry, :"topic_consumer_worker_supervisor_#{topic}"}}
    )
  end

  @doc """
  On startup, determines the partitons to subscribe to from given configuration
  and generates a worker child spec for each.
  """
  def init(args) do
    registry = Keyword.fetch!(args, :registry)
    topic = Keyword.fetch!(args, :topic)
    brod_client = Elsa.Registry.whereis_name({registry, :brod_client})

    Keyword.get_lazy(args, :partition, fn ->
      :brod_client.get_partitions_count(brod_client, topic)
    end)
    |> to_child_specs(args)
    |> Supervisor.init(strategy: :one_for_one)
  end

  defp to_child_specs({:ok, partitions}, args) do
    topic = Keyword.fetch!(args, :topic)

    0..(partitions - 1)
    |> Enum.map(fn partition ->
      name = :"topic_consumer_worker_#{topic}_#{partition}"
      new_args = named_args(name, args) |> Keyword.put(:partition, partition)

      Supervisor.child_spec({Elsa.Consumer.Worker, new_args}, id: name)
    end)
  end

  defp to_child_specs(partition, args) when is_integer(partition) do
    topic = Keyword.fetch!(args, :topic)
    name = :"topic_consumer_worker_#{topic}_#{partition}"

    {Elsa.Consumer.Worker, named_args(name, args)}
    |> Supervisor.child_spec(id: name)
    |> List.wrap()
  end

  defp named_args(name, args) do
    registry = Keyword.fetch!(args, :registry)
    Keyword.put(args, :name, {:via, Elsa.Registry, {registry, name}})
  end
end
