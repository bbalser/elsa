defmodule Elsa.Producer.Supervisor do
  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: name(name))
  end

  def init(opts) do
    children = [
      {Elsa.Producer.Manager, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp name(name), do: :"elsa_producer_supervisor_#{name}"
end
