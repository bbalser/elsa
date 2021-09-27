defmodule Elsa.DynamicProcessManagerTest do
  use ExUnit.Case

  test "will start child process of dynamic supervisor and restart them in case supervisor dies" do
    start_supervised({DynamicSupervisor, strategy: :one_for_one, name: :dyn_sup})

    start_supervised(
      {Elsa.DynamicProcessManager,
       id: :pm,
       name: :pm,
       dynamic_supervisor: :dyn_sup,
       children: [
         %{id: :agent1, start: {Agent, :start_link, [fn -> 0 end, [name: :agent1]]}}
       ]}
    )

    Process.sleep(1_000)
    assert 0 == Agent.get(:agent1, fn s -> s end)

    assert {:ok, _test_server} = Elsa.DynamicProcessManager.start_child(:pm, TestServer)
    assert "hello" == TestServer.echo(TestServer, "hello")

    Process.whereis(:dyn_sup)
    |> Process.exit(:kill)

    Process.sleep(1_000)

    assert "hello again" == TestServer.echo(TestServer, "hello again")
  end

  test "will run initializer function to get initial children" do
    start_supervised({DynamicSupervisor, strategy: :one_for_one, name: :dyn_sup})

    start_supervised(
      {Elsa.DynamicProcessManager,
       id: :pm,
       name: :pm,
       dynamic_supervisor: :dyn_sup,
       initializer: fn ->
         [%{id: :agent1, start: {Agent, :start_link, [fn -> 0 end, [name: :agent1]]}}]
       end}
    )

    Process.sleep(1_000)
    assert 0 == Agent.get(:agent1, fn s -> s end)

    assert {:ok, _test_server} = Elsa.DynamicProcessManager.start_child(:pm, TestServer)
    assert "hello" == TestServer.echo(TestServer, "hello")

    Process.whereis(:dyn_sup)
    |> Process.exit(:kill)

    Process.sleep(1_000)

    assert "hello again" == TestServer.echo(TestServer, "hello again")
  end

  test "when initializer raises an error, it will retry until successful" do
    Agent.start_link(fn -> 7 end, name: :retry_counter)
    start_supervised({DynamicSupervisor, strategy: :one_for_one, name: :dyn_sup})

    start_supervised({
      Elsa.DynamicProcessManager,
      id: :pm, name: :pm, dynamic_supervisor: :dyn_sup, initializer: {TestInitializer, :initialize, [self()]}
    })

    Enum.each(7..1, fn i -> assert_receive {:attempt, ^i}, 2_000 end)
    Process.sleep(2_000)
    assert 0 == Agent.get(:agent1, fn s -> s end)
  end

  test "when configured to poll will start any new children that are returned by initializer" do
    Agent.start_link(fn -> 2 end, name: :retry_counter)
    start_supervised({DynamicSupervisor, strategy: :one_for_one, name: :dyn_sup})

    {:ok, pm} =
      start_supervised({
        Elsa.DynamicProcessManager,
        id: :pm,
        name: :pm,
        dynamic_supervisor: :dyn_sup,
        initializer: {PollingInitializer, :initialize, []},
        poll: 1_000
      })

    Elsa.DynamicProcessManager.ready?(pm)

    assert true == alive?(:agent1)
    assert 1 == Agent.get(:agent1, fn s -> s end)
    assert false == alive?(:agent2)

    Process.sleep(3_000)

    assert true == alive?(:agent1)
    assert 1 == Agent.get(:agent1, fn s -> s end)
    assert true == alive?(:agent2)
    assert 2 == Agent.get(:agent2, fn s -> s end)
  end

  def alive?(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> false
      pid when is_pid(pid) -> Process.alive?(pid)
    end
  end
end

defmodule TestInitializer do
  def initialize(pid) do
    case Agent.get_and_update(:retry_counter, fn s -> {s, s - 1} end) do
      0 ->
        [%{id: :agent1, start: {Agent, :start_link, [fn -> 0 end, [name: :agent1]]}}]

      x ->
        send(pid, {:attempt, x})
        raise "Remaining retries, #{x}"
    end
  end
end

defmodule PollingInitializer do
  def initialize() do
    specs = [
      %{id: :agent1, start: {Agent, :start_link, [fn -> 1 end, [name: :agent1]]}}
    ]

    case Agent.get_and_update(:retry_counter, fn s -> {s, s - 1} end) do
      0 ->
        specs ++
          [
            %{id: :agent2, start: {Agent, :start_link, [fn -> 2 end, [name: :agent2]]}}
          ]

      _ ->
        specs
    end
  end
end

defmodule TestServer do
  use GenServer

  def echo(server, string) do
    GenServer.call(server, {:echo, string})
  end

  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def init(_init_arg) do
    {:ok, %{}}
  end

  def handle_call({:echo, input}, _from, state) do
    {:reply, input, state}
  end
end
