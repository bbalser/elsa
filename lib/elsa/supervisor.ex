defmodule Elsa.Supervisor do
  use Supervisor

  def registry(name) do
    :"elsa_registry_#{name}"
  end

  def supervisor(name) do
    :"elsa_supervisor_#{name}"
  end

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
        start_producer(registry, Keyword.get(args, :producer))
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
