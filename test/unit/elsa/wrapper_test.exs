defmodule Elsa.WrapperTest do
  use ExUnit.Case
  import TestHelper

  @moduletag capture_log: true
  @registry :test_registry

  defmodule NoShutdownServer do
    use GenServer

    def start_link(args) do
      name = Keyword.fetch!(args, :name)
      GenServer.start_link(__MODULE__, args, name: name)
    end

    def init(_args) do
      Process.flag(:trap_exit, true)
      {:ok, []}
    end

    def terminate(reason, _state) do
      Process.sleep(30_000)
      reason
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    {:ok, pid} = Elsa.Registry.start_link(name: @registry)

    on_exit(fn -> assert_down(pid) end)

    :ok
  end

  test "starts configured process" do
    {:ok, pid} = Elsa.Wrapper.start_link(mfa: {Agent, :start_link, [fn -> :agent_state end, [name: :agent_0]]})

    assert :agent_state = Agent.get(:agent_0, fn s -> s end)
    assert_down(pid)
  end

  test "adds an artificial delay when wrapped process dies" do
    {:ok, pid} =
      Elsa.Wrapper.start_link(delay: 2_000, mfa: {Agent, :start_link, [fn -> :agent_state end, [name: :agent_0]]})

    Process.whereis(:agent_0) |> Process.exit(:kill)

    refute_receive {:EXIT, ^pid, :killed}, 1_800
    assert_receive {:EXIT, ^pid, :killed}, 2_000
  end

  test "shuts process down when ask to stop" do
    {:ok, pid} = Elsa.Wrapper.start_link(mfa: {Agent, :start_link, [fn -> :agent_state end, [name: :agent_0]]})
    agent_pid = Process.whereis(:agent_0)

    Process.exit(pid, :shutdown)

    assert_receive {:EXIT, ^pid, :shutdown}
    assert false == Process.alive?(agent_pid)
  end

  test "kills process if it refuses to shutdown" do
    {:ok, pid} = Elsa.Wrapper.start_link(mfa: {NoShutdownServer, :start_link, [[name: :no_shutdown_server]]})
    server_pid = Process.whereis(:no_shutdown_server)

    Process.exit(pid, :shutdown)

    assert_receive {:EXIT, ^pid, :shutdown}, 6_000
    assert false == Process.alive?(server_pid)
  end

  test "registers process with elsa registry" do
    {:ok, pid} =
      Elsa.Wrapper.start_link(mfa: {Agent, :start_link, [fn -> :agent_state end]}, register: {@registry, :agent})

    assert :agent_state = Agent.get({:via, Elsa.Registry, {@registry, :agent}}, fn s -> s end)

    assert_down(pid)
  end
end
