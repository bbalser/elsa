defmodule Elsa.Consumer.WorkerTest do
  use ExUnit.Case
  use Divo

  @endpoints Application.get_env(:elsa, :brokers)

  test "simply consumes messages from configured topic/partition" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "simple-topic")
      end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, pid} =
      Elsa.Supervisor.start_link(
        connection: :test_simple_consumer,
        endpoints: @endpoints,
        consumer: [
          topic: "simple-topic",
          partition: 0,
          begin_offset: :earliest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    Elsa.produce(@endpoints, "simple-topic", {"key1", "value1"}, partition: 0)
    assert_receive [%Elsa.Message{value: "value1"}], 5_000

    Supervisor.stop(pid)
  end

  test "consumes only from configured partition" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "simple-partitioned-topic", partitions: 3)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, pid} =
      Elsa.Supervisor.start_link(
        connection: :test_simple_consumer_partition,
        endpoints: @endpoints,
        consumer: [
          topic: "simple-partitioned-topic",
          partition: 1,
          begin_offset: :earliest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    Elsa.produce(@endpoints, "simple-partitioned-topic", {"0", "zero"}, partition: 0)
    Elsa.produce(@endpoints, "simple-partitioned-topic", {"1", "one"}, partition: 1)
    Elsa.produce(@endpoints, "simple-partitioned-topic", {"2", "two"}, partition: 2)

    assert_receive [%Elsa.Message{value: "one"}], 5_000
    refute_receive [%Elsa.Message{value: "zero"}]
    refute_receive [%Elsa.Message{value: "two"}]

    Supervisor.stop(pid)
  end

  test "can be configured to consume the latest messages only" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "latest-only-topic", partitions: 1)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    Elsa.produce(@endpoints, "latest-only-topic", {"0", "strike 1"}, partition: 0)
    Elsa.produce(@endpoints, "latest-only-topic", {"1", "strike 2"}, partition: 0)

    {:ok, pid} =
      Elsa.Supervisor.start_link(
        connection: :test_simple_consumer_partition,
        endpoints: @endpoints,
        consumer: [
          topic: "latest-only-topic",
          partition: 0,
          begin_offset: :latest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()],
          config: [offset_reset_policy: :earliest]
        ]
      )

    Elsa.produce(@endpoints, "latest-only-topic", {"2", "homerun"}, partition: 0)

    assert_receive [%Elsa.Message{value: "homerun"}], 5_000
    refute_receive [%Elsa.Message{value: "strike 1"}]
    refute_receive [%Elsa.Message{value: "strike 2"}]

    Supervisor.stop(pid)
  end

  test "can be configured to consume from a specific offset" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "specific-offset", partitions: 1)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    Elsa.produce(@endpoints, "specific-offset", {"0", "a"}, partition: 0)
    Elsa.produce(@endpoints, "specific-offset", {"1", "b"}, partition: 0)
    Elsa.produce(@endpoints, "specific-offset", {"2", "c"}, partition: 0)

    {:ok, pid} =
      Elsa.Supervisor.start_link(
        connection: :test_simple_consumer_partition,
        endpoints: @endpoints,
        consumer: [
          topic: "specific-offset",
          partition: 0,
          begin_offset: 1,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    assert_receive [%Elsa.Message{value: "b"}, %Elsa.Message{value: "c"}], 5_000
    refute_receive [%Elsa.Message{value: "a"}]

    Supervisor.stop(pid)
  end

  test "defaults to consuming from all partitions" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "all-partitions", partitions: 2)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    Elsa.produce(@endpoints, "all-partitions", {"0", "a"}, partition: 0)
    Elsa.produce(@endpoints, "all-partitions", {"1", "b"}, partition: 1)

    {:ok, pid} =
      Elsa.Supervisor.start_link(
        connection: :test_simple_consumer_partition,
        endpoints: @endpoints,
        consumer: [
          topic: "all-partitions",
          begin_offset: :earliest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    assert_receive [%Elsa.Message{value: "a"}], 5_000
    assert_receive [%Elsa.Message{value: "b"}], 5_000

    Supervisor.stop(pid)
  end
end

defmodule MyMessageHandler do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages, state) do
    send(state[:pid], messages)
    {:ack, state}
  end
end
