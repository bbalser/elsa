defmodule Elsa.Group.Supervisor do
  @moduledoc """
  Orchestrates the creation of dynamic supervisor and worker
  processes for per-topic consumer groups, manager processes
  for coordinating topic/partition assignment, and a registry
  for differentiating named processes between consumer groups.
  """
  use Supervisor, restart: :transient

  def start_link(init_arg \\ []) do
    name = Keyword.fetch!(init_arg, :name)
    supervisor_name = supervisor_name(name)
    Supervisor.start_link(__MODULE__, init_arg, name: supervisor_name)
  end

  @impl Supervisor
  def init(init_arg) do
    name = Keyword.fetch!(init_arg, :name)
    registry_name = registry(name)

    children = [
      {Registry, [keys: :unique, name: registry_name]},
      {DynamicSupervisor, [strategy: :one_for_one, name: {:via, Registry, {registry_name, :worker_supervisor}}]},
      {Elsa.Group.Manager, manager_args(init_arg)}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc """
  Provide a name-spaced process registry identifier for
  differentiating processes between consumer groups.
  """
  @spec registry(String.t()) :: atom()
  def registry(name) do
    :"elsa_registry_#{name}"
  end

  @doc """
  Ensures descendant workers have unsubscribed and unlinked from supervision
  before gracefully terminating the supervisor process and all remaining children.
  """
  @spec stop(String.t()) :: :ok
  def stop(name) do
    supervisor_name = supervisor_name(name)

    Supervisor.which_children(supervisor_name)
    |> Enum.map(fn {module, manager_pid, _, _} ->
      case module do
        Elsa.Group.Manager ->
          Elsa.Group.Manager.assignments_revoked(manager_pid)

        _ ->
          :ok
      end
    end)

    Supervisor.stop(supervisor_name, {:shutdown, "completed work for this consumer group"})
  end

  defp supervisor_name(name), do: :"elsa_supervisor_#{name}"

  defp manager_args(args) do
    args
    |> Keyword.put(:supervisor_pid, self())
  end
end
