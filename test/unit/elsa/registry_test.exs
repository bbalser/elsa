defmodule Elsa.RegistryTest do
  use ExUnit.Case

  @registry :elsa_registry

  defmodule TestServer do
    use GenServer

    def start_link(args) do
      name = Keyword.fetch!(args, :name)
      GenServer.start_link(__MODULE__, args, name: name)
    end

    def init(args) do
      {:ok, Map.new(args)}
    end

    def handle_info(msg, state) do
      send(state.pid, msg)
      {:noreply, state}
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    {:ok, pid} = Elsa.Registry.start_link(name: @registry)

    on_exit(fn -> assert_down(pid) end)

    :ok
  end

  test "registers and lookups pids by name" do
    Agent.start_link(fn -> :agent_value end, name: {:via, Elsa.Registry, {@registry, :agent}})

    assert :agent_value == Agent.get({:via, Elsa.Registry, {@registry, :agent}}, fn s -> s end)
  end

  test "pid is automatically removed when process exits" do
    {:ok, pid} = Agent.start_link(fn -> :agent_value end, name: {:via, Elsa.Registry, {@registry, :agent}})

    Process.exit(pid, :shutdown)

    Process.sleep(1_000)

    assert false == Process.alive?(pid)
    assert :undefined == Elsa.Registry.whereis_name({@registry, :agent})
  end

  test "pid can be registered by another process" do
    {:ok, pid} = Agent.start_link(fn -> :agent_value end)
    Elsa.Registry.register_name({@registry, :agent}, pid)

    assert :agent_value == Agent.get({:via, Elsa.Registry, {@registry, :agent}}, fn s -> s end)
  end

  test "pid can be unregistered" do
    {:ok, pid} = Agent.start_link(fn -> :agent_value end, name: {:via, Elsa.Registry, {@registry, :agent}})

    Elsa.Registry.unregister_name({@registry, :agent})

    assert :undefined == Elsa.Registry.whereis_name({@registry, :agent})
    assert_down(pid)
  end

  test "send will send a message to pid registered by key" do
    {:ok, pid} = TestServer.start_link(pid: self(), name: {:via, Elsa.Registry, {@registry, :test_server}})

    Elsa.Registry.send({@registry, :test_server}, :hello)

    assert_receive :hello
    assert_down(pid)
  end

  defp assert_down(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
