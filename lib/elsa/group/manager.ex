defmodule Elsa.Group.Manager do
  @moduledoc """
  Defines the GenServer process that coordinates assignment
  of workers to topics/partitions of a given consumer group.
  Tracks consumer group state and reinstantiates workers to
  the last unacknowledged message in the event of failure.
  """
  use GenServer
  require Logger
  import Record, only: [defrecord: 2, extract: 2]
  import Elsa.Supervisor, only: [registry: 1]
  alias Elsa.Group.Manager.WorkerManager

  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  @behaviour :brod_group_member

  @type group :: String.t()
  @type generation_id :: pos_integer()

  @typedoc "Module that implements the Elsa.Consumer.MessageHandler behaviour"
  @type handler :: module()

  @typedoc "Function called for each new assignment"
  @type assignment_received_handler ::
          (group(), Elsa.topic(), Elsa.partition(), generation_id() -> :ok | {:error, term()})

  @typedoc "Function called for when assignments have been revoked"
  @type assignments_revoked_handler :: (() -> :ok)

  @typedoc "Minimum bytes to fetch in batch of messages: default = 0"
  @type min_bytes :: non_neg_integer()

  @typedoc "Maximum bytes to fetch in batch of messages: default = 1MB"
  @type max_bytes :: non_neg_integer()

  @typedoc "Max number of milliseconds to wait to wait for broker to collect min_bytes of messages: default = 10_000 ms"
  @type max_wait_time :: non_neg_integer()

  @typedoc "Allow consumer process to sleep this amount of ms if kafka replied with 'empty' messages: default = 1_000 ms"
  @type sleep_timeout :: non_neg_integer()

  @typedoc "The windows size (number of messages) allowed to fetch-ahead: default = 10"
  @type prefetch_count :: non_neg_integer()

  @typedoc "The total number of bytes allowed to fetch-ahead: default = 100KB"
  @type prefetch_bytes :: non_neg_integer()

  @typedoc "The offset from wthich to begin fetch requests: default = latest"
  @type begin_offset :: non_neg_integer()

  @typedoc "How to reset begin_offset if OffsetOutOfRange exception is received"
  @type offset_reset_policy :: :reset_to_earliest | :reset_to_latest

  @typedoc "Values to configure the consumer, all are optional"
  @type consumer_config :: [
          min_bytes: min_bytes(),
          max_bytes: max_bytes(),
          max_wait_time: max_wait_time(),
          sleep_timeout: sleep_timeout(),
          prefetch_count: prefetch_count(),
          prefetch_bytes: prefetch_bytes(),
          begin_offset: begin_offset(),
          offset_reset_policy: offset_reset_policy()
        ]

  @typedoc "keyword list of config values to start elsa consumer"
  @type start_config :: [
          connection: Elsa.connection(),
          endpoints: Elsa.endpoints(),
          group: group(),
          topics: [Elsa.topic()],
          assignment_received_handler: assignment_received_handler(),
          assignments_revoked_handler: assignments_revoked_handler(),
          handler: handler(),
          handler_init_args: term(),
          config: consumer_config()
        ]

  defmodule State do
    @moduledoc """
    The running state of the consumer group manager process.
    """
    defstruct [
      :connection,
      :group,
      :topics,
      :config,
      :group_coordinator_pid,
      :supervisor_pid,
      :assignment_received_handler,
      :assignments_revoked_handler,
      :handler,
      :handler_init_args,
      :workers,
      :generation_id
    ]
  end

  @doc """
  Provides convenience for backward compatibility with previous versions of Elsa where acking for
  a consumer group was handled by the Elsa.Group.Manager module.
  """
  defdelegate ack(connection, topic, partition, generation_id, offset), to: Elsa.Group.Acknowledger

  def get_committed_offsets(_pid, _topic) do
    {:ok, []}
  end

  @doc """
  Trigger the assignment of workers to a given topic and partition
  """
  @spec assignments_received(pid(), term(), generation_id(), [tuple()]) :: :ok
  def assignments_received(pid, group_member_id, generation_id, assignments) do
    GenServer.call(pid, {:process_assignments, group_member_id, generation_id, assignments})
  end

  @doc """
  Trigger deallocation of all workers from the consumer group and stop
  worker processes.
  """
  @spec assignments_revoked(pid()) :: :ok
  def assignments_revoked(pid) do
    GenServer.call(pid, :revoke_assignments, 30_000)
  end

  @doc """
  Start the group manager process and register a name with the process registry.
  """
  @spec start_link(start_config()) :: GenServer.on_start()
  def start_link(opts) do
    connection = Keyword.fetch!(opts, :connection)
    GenServer.start_link(__MODULE__, opts, name: {:via, Elsa.Registry, {registry(connection), __MODULE__}})
  end

  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %State{
      group: Keyword.fetch!(opts, :group),
      connection: Keyword.fetch!(opts, :connection),
      topics: Keyword.fetch!(opts, :topics),
      supervisor_pid: Keyword.fetch!(opts, :supervisor_pid),
      assignment_received_handler: Keyword.get(opts, :assignment_received_handler, fn _g, _t, _p, _gen -> :ok end),
      assignments_revoked_handler: Keyword.get(opts, :assignments_revoked_handler, fn -> :ok end),
      handler: Keyword.fetch!(opts, :handler),
      handler_init_args: Keyword.get(opts, :handler_init_args, %{}),
      config: Keyword.get(opts, :config, []),
      workers: %{}
    }

    {:ok, state, {:continue, :start_coordinator}}
  end

  def handle_continue(:start_coordinator, state) do
    {:ok, group_coordinator_pid} =
      :brod_group_coordinator.start_link(state.connection, state.group, state.topics, state.config, __MODULE__, self())

    Elsa.Registry.register_name({registry(state.connection), :brod_group_coordinator}, group_coordinator_pid)

    {:noreply, %{state | group_coordinator_pid: group_coordinator_pid}}
  catch
    :exit, reason ->
      wait_and_stop(reason, state)
  end

  def handle_call({:process_assignments, _member_id, generation_id, assignments}, _from, state) do
    case call_lifecycle_assignment_received(state, assignments, generation_id) do
      {:error, reason} ->
        {:stop, reason, {:error, reason}, state}

      :ok ->
        Elsa.Group.Acknowledger.update_generation_id(
          {:via, Elsa.Registry, {registry(state.connection), Elsa.Group.Acknowledger}},
          generation_id
        )

        new_workers = start_workers(state, generation_id, assignments)
        {:reply, :ok, %{state | workers: new_workers, generation_id: generation_id}}
    end
  end

  def handle_call(:revoke_assignments, _from, state) do
    Logger.info("Assignments revoked for group #{state.group}")
    new_workers = WorkerManager.stop_all_workers(state.workers)
    :ok = apply(state.assignments_revoked_handler, [])
    {:reply, :ok, %{state | workers: new_workers, generation_id: nil}}
  end

  def handle_info({:DOWN, ref, :process, _object, _reason}, state) do
    new_workers = WorkerManager.restart_worker(state.workers, ref, state)

    {:noreply, %{state | workers: new_workers}}
  end

  def handle_info({:EXIT, _from, reason}, state) do
    wait_and_stop(reason, state)
  end

  def terminate(reason, %{group_coordinator_pid: group_coordinator_pid} = state) do
    Logger.info("#{__MODULE__} : Terminating #{state.connection}")
    WorkerManager.stop_all_workers(state.workers)

    if group_coordinator_pid != nil && Process.alive?(group_coordinator_pid) do
      Process.exit(group_coordinator_pid, reason)
    end

    reason
  end

  defp call_lifecycle_assignment_received(state, assignments, generation_id) do
    Enum.reduce_while(assignments, :ok, fn brod_received_assignment(topic: topic, partition: partition), :ok ->
      case apply(state.assignment_received_handler, [state.group, topic, partition, generation_id]) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp start_workers(state, generation_id, assignments) do
    Enum.reduce(assignments, state.workers, fn assignment, workers ->
      WorkerManager.start_worker(workers, generation_id, assignment, state)
    end)
  end

  defp wait_and_stop(reason, state) do
    Process.sleep(2_000)
    {:stop, reason, state}
  end
end
