defmodule Elsa.Group.Manager do
  use GenServer
  require Logger
  import Record, only: [defrecord: 2, extract: 2]
  import Elsa.Group.Supervisor, only: [registry: 1]

  @behaviour :brod_group_member

  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  defmodule State do
    defstruct [
      :brokers,
      :name,
      :group,
      :topics,
      :config,
      :client_pid,
      :group_coordinator_pid,
      :supervisor_pid,
      :handler,
      :handler_init_args
    ]
  end

  def get_committed_offsets(_pid, _topic) do
    :ok
  end

  def assignments_received(pid, _group, generation_id, assignments) do
    GenServer.cast(pid, {:process_assignments, generation_id, assignments})
  end

  def assignments_revoked(pid) do
    Logger.error("Assignments revoked : #{inspect(pid)}")
    :ok
  end

  def start_link(opts) do
    group = Keyword.fetch!(opts, :group)
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {registry(group), __MODULE__}})
  end

  def init(opts) do
    group = Keyword.fetch!(opts, :group)

    state = %State{
      brokers: Keyword.fetch!(opts, :brokers),
      group: group,
      name: :"elsa_client_#{group}",
      topics: Keyword.fetch!(opts, :topics),
      supervisor_pid: Keyword.fetch!(opts, :supervisor_pid),
      handler: Keyword.fetch!(opts, :handler),
      handler_init_args: Keyword.get(opts, :handler_init_args, %{}),
      config: Keyword.get(opts, :config, [])
    }

    {:ok, client_pid} =
      state.brokers
      |> Elsa.Util.reformat_endpoints()
      |> :brod.start_link_client(state.name)

    {:ok, %{state | client_pid: client_pid}, {:continue, :start_coordinator}}
  end

  def handle_continue(:start_coordinator, state) do
    {:ok, group_coordinator_pid} =
      :brod_group_coordinator.start_link(state.name, state.group, state.topics, state.config, __MODULE__, self())

    Enum.each(state.topics, fn topic ->
      :ok = :brod.start_consumer(state.name, topic, [])
    end)

    Registry.put_meta(registry(state.group), :group_coordinator, group_coordinator_pid)

    {:noreply, %{state | group_coordinator_pid: group_coordinator_pid}}
  end

  def handle_cast({:process_assignments, generation_id, assignments}, state) do
    assignments
    |> Enum.map(&Enum.into(brod_received_assignment(&1), %{}))
    |> Enum.each(fn assignment ->
      init_args = [
        group: state.group,
        generation_id: generation_id,
        topic: assignment.topic,
        partition: assignment.partition,
        begin_offset: assignment.begin_offset,
        handler: state.handler,
        handler_init_args: state.handler_init_args,
        name: state.name
      ]

      supervisor = {:via, Registry, {registry(state.group), :worker_supervisor}}
      DynamicSupervisor.start_child(supervisor, {Elsa.Group.Worker, init_args})
    end)

    {:noreply, state}
  end
end
