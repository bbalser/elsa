defmodule Elsa.ShutdownTest do
  use ExUnit.Case
  use Divo
  import AssertAsync

  @endpoints [localhost: 9092]

  test "non direct ACKs duplicate data" do
    setup_topic("shutdown-topic")
    test_pid = self()
    options = elsa_options("shutdown-topic", test_pid)

    {:ok, first_run_pid} = start_supervised({Elsa.Supervisor, options})
    assert_receive {:message, %Elsa.Message{value: "a"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "b"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "c"}}, 5_000

    stop_supervised(Elsa.Supervisor)

    assert_async sleep: 1_000, max_tries: 60 do
      assert false == Process.alive?(first_run_pid)
    end

    {:ok, _second_run_pid} = start_supervised({Elsa.Supervisor, options})
    # give it time to pull in duplicates if they are there
    Process.sleep(10_000)

    refute_receive {:message, _}, 10_000
  end

  defp setup_topic(topic) do
    Elsa.create_topic(@endpoints, topic)

    assert_async do
      assert Elsa.topic?(@endpoints, topic)
    end

    Elsa.produce(@endpoints, topic, ["a", "b", "c"])
  end

  defp elsa_options(topic, test_pid) do
    [
      name: :"#{topic}_consumer",
      endpoints: @endpoints,
      connection: :"#{topic}",
      group_consumer: [
        group: "test-#{topic}",
        topics: [topic],
        handler: FakeMessageHandler,
        handler_init_args: [test_pid: test_pid],
        config: [
          begin_offset: :earliest,
          offset_reset_policy: :reset_to_earliest,
          max_bytes: 1_000_000,
          min_bytes: 0,
          max_wait_time: 10_000
        ]
      ]
    ]
  end
end

defmodule FakeMessageHandler do
  use Elsa.Consumer.MessageHandler

  def init(args) do
    {:ok, %{test_pid: Keyword.fetch!(args, :test_pid)}}
  end

  def handle_messages(messages, %{test_pid: test_pid} = state) do
    IO.inspect(messages, label: "processing")

    Enum.each(messages, &send(test_pid, {:message, &1}))
    # pretend we're doing some heavy lifting here
    Process.sleep(5_000)

    IO.inspect(messages, label: "acking")
    {:ack, state}
  end
end
