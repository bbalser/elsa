defmodule Elsa.Group.Member do
  use GenServer
  @behaviour :brod_group_member

  defmodule State do
    defstruct [:name, :group, :topics, :config, :group_coordinator_pid]
  end

  def get_committed_offsets(_pid, _topic) do
    :ok
  end

  def assignments_received(_pid, _group, _generation_id, _assignments) do
    :ok
  end

  def assignments_revoked(_pid) do
    :ok
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = %State{
      name: Keyword.fetch!(opts, :name),
      group: Keyword.fetch!(opts, :group),
      topics: Keyword.fetch!(opts, :topics),
      config: Keyword.get(opts, :config, [])
    }

    {:ok, state, {:continue, :start_coordinator}}
  end

  def handle_continue(:start_coordinator, state) do
    {:ok, group_coordinator_pid} =
      :brod_group_coordinator.start_link(state.name, state.group, state.topics, state.config, __MODULE__, self())

    {:noreply, %{state | group_coordinator_pid: group_coordinator_pid}}
  end
end
