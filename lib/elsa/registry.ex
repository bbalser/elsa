defmodule Elsa.Registry do
  @moduledoc """
  Implements a custom version of the Registry
  for Elsa, allowing the registration of shared
  processes like brod clients as well as processes
  started under brod supervision.

  Saves the process identifier-to-name key/value pairs
  to ETS.
  """
  use GenServer
  require Logger

  @doc """
  Register the pid of a process to the registry under
  a given name.
  """
  @spec register_name({atom(), term()}, pid()) :: :yes
  def register_name({registry, key}, pid) do
    GenServer.call(registry, {:register, key, pid})
  end

  @doc """
  De-register the process name from its associated pid
  within the registry.
  """
  @spec unregister_name({atom(), term()}) :: :ok
  def unregister_name({registry, key}) do
    GenServer.call(registry, {:unregister, key})
  end

  @doc """
  Lookup a pid from within the registry by name.
  """
  @spec whereis_name({atom(), term()}) :: pid() | :undefined
  def whereis_name({registry, key}) do
    case :ets.lookup(registry, key) do
      [{^key, pid}] -> pid
      [] -> :undefined
    end
  end

  @doc """
  Select all records within the registry and return them as a list.
  """
  @spec select_all(atom() | :ets.tid()) :: [tuple()]
  def select_all(registry) do
    :ets.tab2list(registry)
  end

  @doc """
  Wraps the Kernel module `send/2` function with a safe call to
  ensure the receiving process is defined.
  """
  @spec send({atom(), term()}, term()) :: term()
  def send({registry, key}, msg) do
    case whereis_name({registry, key}) do
      :undefined -> :erlang.error(:badarg, [{registry, key}, msg])
      pid -> Kernel.send(pid, msg)
    end
  end

  @doc """
  Start the Elsa registery process and link it to the current process.
  Creates the process registry table in ETS and traps exits during the
  init process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
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
