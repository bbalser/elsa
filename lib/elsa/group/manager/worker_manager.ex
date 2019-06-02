defmodule Elsa.Group.Manager.WorkerManager do
  import Record, only: [defrecord: 2, extract: 2]
  import Elsa.Group.Supervisor, only: [registry: 1]

  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  defmodule WorkerState do
    defstruct [:pid, :ref, :generation_id, :topic, :partition, :latest_offset]
  end

  def get_generation_id(workers, topic, partition) do
    Map.get(workers, {topic, partition})
    |> Map.get(:generation_id)
  end

  def update_offset(workers, topic, partition, offset) do
    Map.update!(workers, {topic, partition}, fn worker -> %{worker | latest_offset: offset} end)
  end

  def restart_worker(workers, ref, %Elsa.Group.Manager.State{} = state) do
    worker = get_by_ref(workers, ref)
    assignment = brod_received_assignment(topic: worker.topic, partition: worker.partition, begin_offset: worker.latest_offset + 1)
    start_worker(workers, worker.generation_id, assignment, state)
  end

  def start_worker(workers, generation_id, assignment, %Elsa.Group.Manager.State{} = state) do
    assignment = Enum.into(brod_received_assignment(assignment), %{})

    init_args = [
      topic: assignment.topic,
      partition: assignment.partition,
      begin_offset: assignment.begin_offset,
      handler: state.handler,
      handler_init_args: state.handler_init_args,
      name: state.name
    ]

    supervisor = {:via, Registry, {registry(state.name), :worker_supervisor}}
    {:ok, worker_pid} = DynamicSupervisor.start_child(supervisor, {Elsa.Group.Worker, init_args})
    ref = Process.monitor(worker_pid)

    new_worker = %WorkerState{
      pid: worker_pid,
      ref: ref,
      generation_id: generation_id,
      topic: assignment.topic,
      partition: assignment.partition,
      latest_offset: assignment.begin_offset
    }

    Map.put(workers, {assignment.topic, assignment.partition}, new_worker)
  end

  defp get_by_ref(workers, ref) do
    workers
    |> Map.values()
    |> Enum.find(fn worker -> worker.ref == ref end)
  end
end
