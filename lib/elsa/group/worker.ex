defmodule Elsa.Group.Worker do
  use GenServer

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init(init_args) do
    {:ok, []}
  end
end
