defmodule Elsa.DynamicProcessManager do
  use GenServer
  require Logger

  @type child_spec :: Supervisor.child_spec() | {module, term} | module

  def start_child(server, child_spec) do
    GenServer.call(server, {:start_child, child_spec})
  end

  def start_link(init_arg) do
    server_opts = Keyword.take(init_arg, [:name])
    GenServer.start_link(__MODULE__, init_arg, server_opts)
  end

  def child_spec(init_arg) do
    id = Keyword.fetch!(init_arg, :id)

    %{
      id: id,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  def ready?(server, timeout \\ 10_000) do
    GenServer.call(server, :ready?, timeout)
  end

  def init(init_arg) do
    Process.flag(:trap_exit, true)

    dynamic_supervisor = Keyword.fetch!(init_arg, :dynamic_supervisor)

    initializer =
      case Keyword.get(init_arg, :initializer) do
        {module, function, args} ->
          fn -> apply(module, function, args) end

        function when is_function(function, 0) ->
          function

        _ ->
          fn -> [] end
      end

    state = %{
      dynamic_supervisor: dynamic_supervisor,
      dynamic_supervisor_ref: whereis(dynamic_supervisor) |> Process.monitor(),
      poll: Keyword.get(init_arg, :poll, false),
      initializer: initializer,
      child_specs: Keyword.get(init_arg, :children, [])
    }

    {:ok, state, {:continue, :initialize}}
  end

  def handle_continue(:initialize, state) do
    start_children(state.dynamic_supervisor, state.child_specs)

    new_state = start_new_children(state)
    setup_poll(state.poll)

    {:noreply, new_state}
  end

  def handle_call({:start_child, child}, _from, state) do
    output = DynamicSupervisor.start_child(state.dynamic_supervisor, child)
    {:reply, output, Map.update!(state, :child_specs, fn specs -> specs ++ [child] end)}
  end

  # When handle_continue has completed this process can
  # reply to this message and is therefore ready.
  def handle_call(:ready?, _from, state) do
    {:reply, true, state}
  end

  def handle_info(:poll, state) do
    Logger.debug(fn -> "#{__MODULE__} for #{inspect(state.dynamic_supervisor)}: Polling for new children" end)
    new_state = start_new_children(state)
    setup_poll(state.poll)
    {:noreply, new_state}
  end

  def handle_info({:DOWN, ref, _, _, _}, %{dynamic_supervisor_ref: ref} = state) do
    Logger.info(fn ->
      "#{__MODULE__}: Dynamic Supervisor #{state.dynamic_supervisor} has died, restarting recorded children"
    end)

    wait_for(state.dynamic_supervisor)

    start_children(state.dynamic_supervisor, state.child_specs)
    {:noreply, state}
  end

  defp start_new_children(state) do
    new_child_specs = initialize_until_success(state.initializer) -- state.child_specs
    start_children(state.dynamic_supervisor, new_child_specs)

    Map.update!(state, :child_specs, fn specs ->
      specs ++ new_child_specs
    end)
  end

  defp start_children(supervisor, child_specs) do
    child_specs
    |> Enum.each(fn child ->
      output = DynamicSupervisor.start_child(supervisor, child)

      Logger.debug(fn ->
        "#{__MODULE__}: output of starting #{inspect(child)}: #{inspect(output)}"
      end)
    end)
  end

  defp initialize_until_success(initializer) do
    initializer.()
  rescue
    e ->
      Logger.warn(fn -> "#{__MODULE__}: initializer raised exception, retrying: #{inspect(e)}" end)

      Process.sleep(1_000)
      initialize_until_success(initializer)
  catch
    :exit, e ->
      Logger.warn(fn -> "#{__MODULE__}: initializer raised exception, retrying: #{inspect(e)}" end)

      Process.sleep(1_000)
      initialize_until_success(initializer)
  end

  defp setup_poll(time) when is_integer(time) do
    :timer.send_after(time, :poll)
  end

  defp setup_poll(_), do: nil

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
