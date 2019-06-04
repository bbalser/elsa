defmodule Elsa.Group.Manager do
  use GenServer
  require Logger
  import Elsa.Group.Supervisor, only: [registry: 1]
  alias Elsa.Group.Manager.WorkerManager

  @behaviour :brod_group_member

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
      :handler_init_args,
      :workers
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
    GenServer.cast(pid, :revoke_assignments)
  end

  def ack(name, topic, partition, offset) do
    group_manager = {:via, Registry, {registry(name), __MODULE__}}
    GenServer.cast(group_manager, {:ack, topic, partition, offset})
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {registry(name), __MODULE__}})
  end

  def init(opts) do
    state = %State{
      brokers: Keyword.fetch!(opts, :brokers),
      group: Keyword.fetch!(opts, :group),
      name: Keyword.fetch!(opts, :name),
      topics: Keyword.fetch!(opts, :topics),
      supervisor_pid: Keyword.fetch!(opts, :supervisor_pid),
      handler: Keyword.fetch!(opts, :handler),
      handler_init_args: Keyword.get(opts, :handler_init_args, %{}),
      config: Keyword.get(opts, :config, []),
      workers: %{}
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
      :ok = :brod.start_consumer(state.name, topic, state.config)
    end)

    Registry.put_meta(registry(state.name), :group_coordinator, group_coordinator_pid)

    {:noreply, %{state | group_coordinator_pid: group_coordinator_pid}}
  end

  def handle_cast({:process_assignments, generation_id, assignments}, state) do
    new_workers =
      Enum.reduce(assignments, state.workers, fn assignment, workers ->
        WorkerManager.start_worker(workers, generation_id, assignment, state)
      end)

    {:noreply, %{state | workers: new_workers}}
  end

  def handle_cast(:revoke_assignments, state) do
    new_workers = WorkerManager.stop_all_workers(state.workers)
    {:noreply, %{state | workers: new_workers}}
  end

  def handle_cast({:ack, topic, partition, offset}, state) do
    generation_id = WorkerManager.get_generation_id(state.workers, topic, partition)
    :ok = :brod_group_coordinator.ack(state.group_coordinator_pid, generation_id, topic, partition, offset)
    new_workers = WorkerManager.update_offset(state.workers, topic, partition, offset)
    {:noreply, %{state | workers: new_workers}}
  end

  def handle_info({:DOWN, ref, :process, _object, _reason}, state) do
    new_workers = WorkerManager.restart_worker(state.workers, ref, state)

    {:noreply, %{state | workers: new_workers}}
  end
end
