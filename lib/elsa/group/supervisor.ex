defmodule Elsa.Group.Supervisor do
  use Supervisor

  def start_link(init_arg \\ []) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl Supervisor
  def init(init_arg) do
    children = [
      {DynamicSupervisor, [strategy: :one_for_one]},
      {Elsa.Group.Member, Keyword.put(init_arg, :supervisor_pid, self())}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def worker_supervisor(pid) do
    find_child(pid, DynamicSupervisor)
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
