defmodule Elsa.Group.Supervisor do
  use Supervisor

  def start_link(init_arg \\ []) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl Supervisor
  def init(init_arg) do
    # how to handle the registry??
    children = [
      {Registry, [keys: :unique]},
      {DynamicSupervisor, [strategy: :one_for_one]},
      {Elsa.Group.Member, init_arg}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def registry(pid) do
    find_child(pid, Registry)
  end

  def group_member(pid) do
    find_child(pid, Elsa.Group.Member)
  end

  defp find_child(sup_pid, module) do
    {^module, child_pid, _type, _modules} =
      Supervisor.which_children(sup_pid)
      |> Enum.find(fn child -> match?({^module, _pid, _type, _modules}, child) end)

    child_pid
  end
end
