defmodule Elsa.Group.Supervisor do
  @moduledoc """
  Orchestrates the creation of dynamic supervisor and worker
  processes for per-topic consumer groups, manager processes
  for coordinating topic/partition assignment, and a registry
  for differentiating named processes between consumer groups.
  """
  use Supervisor, restart: :transient

  import Elsa.Supervisor, only: [registry: 1]

  def start_link(init_arg \\ []) do
    connection = Keyword.fetch!(init_arg, :connection)
    Supervisor.start_link(__MODULE__, init_arg, name: {:via, Elsa.Registry, {registry(connection), __MODULE__}})
  end

  @impl Supervisor
  def init(init_arg) do
    connection = Keyword.fetch!(init_arg, :connection)
    topics = Keyword.fetch!(init_arg, :topics)
    config = Keyword.get(init_arg, :config, [])
    registry = registry(connection)

    children =
      [
        {DynamicSupervisor, [strategy: :one_for_one, name: {:via, Elsa.Registry, {registry, :worker_supervisor}}]},
        consumer_supervisors(registry, topics, config),
        {Elsa.Group.Manager, manager_args(init_arg)},
        {Elsa.Group.Acknowledger, init_arg}
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp consumer_supervisors(registry, topics, config) do
    Enum.map(topics, fn topic ->
      {Elsa.Consumer.Supervisor, [registry: registry, topic: topic, config: config]}
    end)
  end

  defp manager_args(args) do
    args
    |> Keyword.put(:supervisor_pid, self())
  end
end
