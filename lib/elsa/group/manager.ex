defmodule Elsa.Group.Manager do
  @moduledoc """
  Defines the GenServer process that coordinates assignment
  of workers to topics/partitions of a given consumer group.
  Tracks consumer group state and reinstantiates workers to
  the last unacknowledged message in the event of failure.
  """
  use GenServer
  require Logger
  import Elsa.Group.Supervisor, only: [registry: 1]
  alias Elsa.Group.Manager.WorkerManager

  @behaviour :brod_group_member

  @type hostname :: atom() | String.t()
  @type portnum :: pos_integer()

  @type topic :: String.t()

  @typedoc "Module that implements the Elsa.Consumer.MessageHandler behaviour"
  @type handler :: module()

  @typedoc "endpoints to connect to kafka brokers"
  @type endpoints :: [{hostname(), portnum()}]

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
          name: atom(),
          endpoints: endpoints(),
          group: String.t(),
          topics: [topic()],
          handler: handler(),
          handler_init_args: term(),
          config: consumer_config()
        ]

  defmodule State do
    @moduledoc """
    The running state of the consumer group manager process.
    """
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
    {:ok, []}
  end

  @doc """
  Trigger the assignment of workers to a given topic and partition
  """
  @spec assignments_received(pid(), term(), integer(), [tuple()]) :: :ok
  def assignments_received(pid, _group, generation_id, assignments) do
    GenServer.cast(pid, {:process_assignments, generation_id, assignments})
  end

  @doc """
  Trigger deallocation of all workers from the consumer group and stop
  worker processes.
  """
  @spec assignments_revoked(pid()) :: :ok
  def assignments_revoked(pid) do
    Logger.error("Assignments revoked : #{inspect(pid)}")
    GenServer.cast(pid, :revoke_assignments)
  end

  @doc """
  Trigger acknowledgement of processed messages back to the cluster.
  """
  @spec ack(String.t(), String.t(), integer(), integer(), integer()) :: :ok
  def ack(name, topic, partition, generation_id, offset) do
    group_manager = {:via, Registry, {registry(name), __MODULE__}}
    GenServer.cast(group_manager, {:ack, topic, partition, generation_id, offset})
  end

  @doc """
  Trigger acknowldgement of processed messages back to the cluster.
  """
  @spec ack(String.t(), %{topic: String.t(), partition: integer(), generation_id: integer(), offset: integer()}) :: :ok
  def ack(name, %{topic: topic, partition: partition, generation_id: generation_id, offset: offset}) do
    ack(name, topic, partition, generation_id, offset)
  end

  @doc """
  Start the group manager process and register a name with the process registry.
  """
  @spec start_link(start_config()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {registry(name), __MODULE__}})
  end

  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %State{
      brokers: Keyword.get_lazy(opts, :brokers, fn -> Keyword.fetch!(opts, :endpoints) end),
      group: Keyword.fetch!(opts, :group),
      name: Keyword.fetch!(opts, :name),
      topics: Keyword.fetch!(opts, :topics),
      supervisor_pid: Keyword.fetch!(opts, :supervisor_pid),
      handler: Keyword.fetch!(opts, :handler),
      handler_init_args: Keyword.get(opts, :handler_init_args, %{}),
      config: Keyword.get(opts, :config, []),
      workers: %{}
    }

    {:ok, state, {:continue, :start_coordinator}}
  end

  def handle_continue(:start_coordinator, state) do
    {:ok, client_pid} = Elsa.Util.start_client(state.brokers, state.name)

    {:ok, group_coordinator_pid} =
      :brod_group_coordinator.start_link(state.name, state.group, state.topics, state.config, __MODULE__, self())

    Enum.each(state.topics, fn topic ->
      :ok = :brod.start_consumer(state.name, topic, state.config)
    end)

    Registry.put_meta(registry(state.name), :group_coordinator, group_coordinator_pid)

    {:noreply, %{state | client_pid: client_pid, group_coordinator_pid: group_coordinator_pid}}
  catch
    :exit, reason ->
      wait_and_stop(reason, state)
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

  def handle_cast({:ack, topic, partition, generation_id, offset}, state) do
    assignment_generation_id = WorkerManager.get_generation_id(state.workers, topic, partition)

    case assignment_generation_id == generation_id do
      true ->
        :ok = :brod_group_coordinator.ack(state.group_coordinator_pid, generation_id, topic, partition, offset)
        :ok = :brod.consume_ack(state.name, topic, partition, offset)
        new_workers = WorkerManager.update_offset(state.workers, topic, partition, offset)
        {:noreply, %{state | workers: new_workers}}

      false ->
        Logger.warn("Invalid generation_id, ignoring ack - topic #{topic} parition #{partition} offset #{offset}")
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _object, _reason}, state) do
    new_workers = WorkerManager.restart_worker(state.workers, ref, state)

    {:noreply, %{state | workers: new_workers}}
  end

  def handle_info({:EXIT, _from, reason}, state) do
    wait_and_stop(reason, state)
  end

  defp wait_and_stop(reason, state) do
    Process.sleep(2_000)
    {:stop, reason, state}
  end
end
