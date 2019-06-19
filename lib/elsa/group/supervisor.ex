defmodule Elsa.Group.Supervisor do
  @moduledoc """
  Orchestrates the creation of dynamic supervisor and worker
  processes for per-topic consumer groups, manager processes
  for coordinating topic/partition assignment, and a registry
  for differentiating named processes between consumer groups.
  """
  use Supervisor

  def start_link(init_arg \\ []) do
    name = Keyword.fetch!(init_arg, :name)
    supervisor_name = :"elsa_supervisor_#{name}"
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

  defp manager_args(args) do
    args
    |> Keyword.put(:supervisor_pid, self())
  end
end
