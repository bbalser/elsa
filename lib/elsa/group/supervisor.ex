defmodule Elsa.Group.Supervisor do
  @moduledoc """
  Orchestrates the creation of dynamic supervisor and worker
  processes for per-topic consumer groups, manager processes
  for coordinating topic/partition assignment, and a registry
  for differentiating named processes between consumer groups.
  """
  use Supervisor, restart: :transient

  import Elsa.Supervisor, only: [registry: 1]

  @type init_opts :: [
          connection: Elsa.connection(),
          topics: [Elsa.topic()],
          group: String.t(),
          config: list
        ]

  @spec start_link(init_opts) :: GenServer.on_start()
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
    poll = Keyword.get(init_arg, :poll, false)

    group_consumer_supervisor = {:via, Elsa.Registry, {registry, :group_consumer_supervisor}}

    children =
      [
        {DynamicSupervisor, [strategy: :one_for_one, name: {:via, Elsa.Registry, {registry, :worker_supervisor}}]},
        {DynamicSupervisor, [strategy: :one_for_one, name: group_consumer_supervisor]},
        start_consumer(registry, topics, config, group_consumer_supervisor, poll),
        {Elsa.Group.Manager, manager_args(init_arg)}
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp start_consumer(registry, topics, config, dynamic_supervisor, poll) do
    {Elsa.DynamicProcessManager,
     id: :group_consumer_process_manager,
     dynamic_supervisor: dynamic_supervisor,
     poll: poll,
     initializer:
       {Elsa.Consumer.Initializer, :init,
        [
          [
            registry: registry,
            topics: topics,
            config: config
          ]
        ]}}
  end

  defp manager_args(args) do
    args
    |> Keyword.put(:supervisor_pid, self())
  end
end
