defmodule Elsa.Supervisor do
  @moduledoc """
  Top-level supervisor that orchestrates all other components
  of the Elsa library. Allows for a single point of integration
  into your application supervision tree and configuration by way
  of a series of nested keyword lists

  Components not needed by a running application (if your application
  _only_ consumes messages from Kafka and never producers back to it)
  can be safely omitted from the configuration.
  """
  use Supervisor

  @doc """
  Defines a name for locating the Elsa Registry process.
  """
  @spec registry(String.t() | atom()) :: atom()
  def registry(name) do
    :"elsa_registry_#{name}"
  end

  @doc """
  Defines a name for locating the primary supervisor.
  """
  @spec supervisor(String.t() | atom()) :: atom()
  def supervisor(name) do
    :"elsa_supervisor_#{name}"
  end

  @doc """
  Starts the top-level Elsa supervisor and links it to the
  current process.
  Starts a brod client and a custom process registry by default
  and then conditionally starts and takes supervision of any
  brod group-based consumers or producer processes defined.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    Supervisor.start_link(__MODULE__, args, name: supervisor(name))
  end

  def init(args) do
    name = Keyword.fetch!(args, :name)
    registry = registry(name)

    children =
      [
        {Elsa.Registry, name: registry},
        start_client(args),
        start_producer(registry, Keyword.get(args, :producer)),
        start_group_consumer(name, registry, Keyword.get(args, :group_consumer))
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp start_client(args) do
    name = Keyword.fetch!(args, :name)
    endpoints = Keyword.fetch!(args, :endpoints)
    config = Keyword.get(args, :config, [])

    {Elsa.Wrapper,
     mfa: {:brod_client, :start_link, [endpoints, name, config]}, register: {registry(name), :brod_client}}
  end

  defp start_group_consumer(_name, _registry, nil), do: []

  defp start_group_consumer(name, registry, args) do
    group_consumer_args =
      args
      |> Keyword.put(:registry, registry)
      |> Keyword.put(:name, name)

    {Elsa.Group.Supervisor, group_consumer_args}
  end

  defp start_producer(_registry, nil), do: []

  defp start_producer(registry, args) do
    case Keyword.keyword?(args) do
      true -> producer_child_spec(registry, args)
      false -> Enum.map(args, fn entry -> producer_child_spec(registry, entry) end)
    end
  end

  defp producer_child_spec(registry, args) do
    topic = Keyword.fetch!(args, :topic)

    producer_args =
      args
      |> Keyword.put(:registry, registry)
      |> Keyword.put(:name, {:via, Elsa.Registry, {registry, :"producer_supervisor_#{topic}"}})

    Supervisor.child_spec({Elsa.Producer.Supervisor, producer_args}, id: :"producer_supervisor_#{topic}")
  end
end
