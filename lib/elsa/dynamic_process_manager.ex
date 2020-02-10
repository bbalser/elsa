defmodule Elsa.DynamicProcessManager do
  use GenServer
  require Logger

  @type child_spec :: Supervisor.child_spec() | {module, term} | module

  def start_child(server, child_spec) do
    GenServer.call(server, {:start_child, child_spec})
  end

  def start_link(init_arg) do
    name = Keyword.get(init_arg, :name, nil)
    GenServer.start_link(__MODULE__, init_arg, name: name)
  end

  def child_spec(init_arg) do
    id = Keyword.fetch!(init_arg, :id)
    %{
      id: id,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  def init(init_arg) do
    Process.flag(:trap_exit, true)

    dynamic_supervisor = Keyword.fetch!(init_arg, :dynamic_supervisor)
    synchronous? = Keyword.get(init_arg, :synchronous, false)

    state = %{
      dynamic_supervisor: dynamic_supervisor,
      dynamic_supervisor_ref: whereis(dynamic_supervisor) |> Process.monitor(),
      child_specs: Keyword.get(init_arg, :children, [])
    }

    initializer =
      case Keyword.get(init_arg, :initializer) do
        {module, function, args} -> fn -> apply(module, function, args) end
        function when is_function(function, 0) -> function
        _ -> fn -> [] end
      end

    case synchronous? do
      false -> {:ok, state, {:continue, {:initialize, initializer}}}
      true ->
        {:noreply, new_state} = handle_continue({:initialize, initializer}, state)
        {:ok, new_state}
    end
  end

  def handle_continue({:initialize, initializer}, state) do
    new_state =
      Map.update!(state, :child_specs, fn specs ->
        specs ++ run_initializer(initializer)
      end)

    start_children(new_state)
    {:noreply, new_state}
  end

  def handle_call({:start_child, child}, _from, state) do
    output = DynamicSupervisor.start_child(state.dynamic_supervisor, child)
    {:reply, output, Map.update!(state, :child_specs, fn specs -> specs ++ [child] end)}
  end

  def handle_info({:DOWN, ref, _, _, _}, %{dynamic_supervisor_ref: ref} = state) do
    Logger.info(fn ->
      "#{__MODULE__}: Dynamic Supervisor #{state.dynamic_supervisor} has died, restarting recorded children"
    end)

    wait_for(state.dynamic_supervisor)

    start_children(state)
    {:noreply, state}
  end

  defp start_children(state) do
    state.child_specs
    |> Enum.each(fn child ->
      output = DynamicSupervisor.start_child(state.dynamic_supervisor, child)

      Logger.debug(fn ->
        "#{__MODULE__}: output of starting #{inspect(child)}: #{inspect(output)}"
      end)
    end)
  end

  defp run_initializer(initializer) do
    initializer.()
  rescue
    e ->
      Logger.warn(fn -> "#{__MODULE__}: initializer raised exception, retrying: #{inspect(e)}" end)

      Process.sleep(1_000)
      run_initializer(initializer)
  end

  defp whereis(name) when is_atom(name) do
    Process.whereis(name)
  end

  defp whereis({:via, registry_module, lookup}) do
    registry_module.whereis_name(lookup)
  end

  defp wait_for(name) do
    case Process.whereis(name) do
      nil ->
        Process.sleep(200)
        wait_for(name)

      pid ->
        case Process.alive?(pid) do
          false ->
            Process.sleep(200)
            wait_for(name)

          true ->
            :ok
        end
    end
  end
end
