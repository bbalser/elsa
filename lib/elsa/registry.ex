defmodule Elsa.Registry do
  use GenServer
  require Logger

  def register_name({registry, key}, pid) do
    GenServer.call(registry, {:register, key, pid})
  end

  def unregister_name({registry, key}) do
    GenServer.call(registry, {:unregister, key})
  end

  def whereis_name({registry, key}) do
    case :ets.lookup(registry, key) do
      [{^key, pid}] -> pid
      [] -> :undefined
    end
  end

  def select_all(registry) do
    :ets.tab2list(registry)
  end

  def send({registry, key}, msg) do
    case whereis_name({registry, key}) do
      :undefined -> :erlang.error(:badarg, [{registry, key}, msg])
      pid -> Kernel.send(pid, msg)
    end
  end

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    GenServer.start_link(__MODULE__, args, name: name)
  end

  def init(args) do
    Process.flag(:trap_exit, true)

    name = Keyword.fetch!(args, :name)

    ^name = :ets.new(name, [:named_table, :set, :protected, {:read_concurrency, true}])

    {:ok, %{registry: name}}
  end

  def handle_call({:register, key, pid}, _from, state) do
    Process.link(pid)
    :ets.insert(state.registry, {key, pid})

    {:reply, :yes, state}
  end

  def handle_call({:unregister, key}, _from, state) do
    case :ets.lookup(state.registry, key) do
      [{^key, pid}] ->
        Process.unlink(pid)
        :ets.delete(state.registry, key)

      _ ->
        :ok
    end

    {:reply, :ok, state}
  end

  def handle_info({:EXIT, pid, _reason}, state) do
    case :ets.match(state.registry, {:"$1", pid}) do
      [[key]] ->
        Logger.debug(fn -> "#{__MODULE__}: Removing key(#{inspect(key)}) and pid(#{inspect(pid)}))" end)
        :ets.delete(state.registry, key)

      _ ->
        :ok
    end

    {:noreply, state}
  end
end
