defmodule Elsa.Group.Member do
  use GenServer
  require Logger
  import Record, only: [defrecord: 2, extract: 2]
  @behaviour :brod_group_member

  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  defmodule State do
    defstruct [:brokers, :name, :group, :topics, :config, :group_coordinator_pid, :supervisor_pid]
  end

  def get_committed_offsets(_pid, _topic) do
    :ok
  end

  def assignments_received(_pid, _group, _generation_id, assignments) do
    :ok
  end

  def assignments_revoked(pid) do
    Logger.error("Assignments revoked : #{inspect(pid)}")
    :ok
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = %State{
      brokers: Keyword.fetch!(opts, :brokers),
      name: Keyword.fetch!(opts, :name),
      group: Keyword.fetch!(opts, :group),
      topics: Keyword.fetch!(opts, :topics),
      supervisor_pid: Keyword.fetch!(opts, :supervisor_pid),
      config: Keyword.get(opts, :config, [])
    }

    :ok =
      state.brokers
      |> Elsa.Util.reformat_endpoints()
      |> :brod.start_client(state.name)

    {:ok, state, {:continue, :start_coordinator}}
  end

  def handle_continue(:start_coordinator, state) do
    {:ok, group_coordinator_pid} =
      :brod_group_coordinator.start_link(state.name, state.group, state.topics, state.config, __MODULE__, self())

    {:noreply, %{state | group_coordinator_pid: group_coordinator_pid}}
  end
end
