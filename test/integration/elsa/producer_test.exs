defmodule Elsa.ProducerTest do
  use ExUnit.Case
  use Divo
  import Checkov
  import TestHelper

  alias Elsa.Producer
  require Elsa.Message

  @brokers [{'localhost', 9092}]
  @moduletag capture_log: true

  describe "producer managers" do
    setup do
      topic = "producer-manager-test"
      topic2 = "producer-test-secondary"
      connection = :elsa_producer_test2

      Elsa.create_topic(@brokers, topic)
      Elsa.create_topic(@brokers, topic2)

      {:ok, supervisor} =
        Elsa.Supervisor.start_link(endpoints: @brokers, connection: connection, producer: [[topic: topic], [topic: topic2]])

      Elsa.Producer.ready?(connection)

      on_exit(fn ->
        assert_down(supervisor)
      end)

      [connection: connection, topics: [topic, topic2], registry: Elsa.Supervisor.registry(connection)]
    end

    test "restarts producers when the client is dropped", %{connection: connection, topics: [topic, topic2], registry: registry} do
      message = "everything's fine here"
      message2 = "also over here"
      client_pid = Elsa.Registry.whereis_name({registry, :brod_client})
      Process.exit(client_pid, :kill)

      Patiently.wait_for!(
        fn ->
          Process.alive?(client_pid) == false and Process.whereis(connection) != nil
        end,
        dwell: 1_000,
        max_tries: 30
      )

      Producer.produce(connection, topic, message)
      Producer.produce(connection, topic2, message2)

      Patiently.wait_for!(
        fn ->
          with {:ok, 1, [%Elsa.Message{value: result}]} <- Elsa.fetch(@brokers, topic),
               true <- message == result,
               {:ok, 1, [%Elsa.Message{value: result2}]} <- Elsa.fetch(@brokers, topic2),
               true <- message2 == result2 do
            true
          else
            _ ->
              false
          end
        end,
        dwell: 1_000,
        max_tries: 30
      )
    end
  end

  describe "preconfigured broker" do
    data_test "produces to topic" do
      Elsa.create_topic(@brokers, topic, partitions: num_partitions)
      connection = String.to_atom(topic)

      {:ok, supervisor} =
        Elsa.Supervisor.start_link(endpoints: @brokers, connection: connection, producer: [topic: topic])

      on_exit(fn -> assert_down(supervisor) end)

      patient_produce(connection, topic, messages, produce_opts)

      parsed_messages = retrieve_results(@brokers, topic, Keyword.get(produce_opts, :partition, 0), 0)

      expected = if expected_messages == :same, do: messages, else: expected_messages

      assert expected == parsed_messages

      where [
        [:topic, :num_partitions, :messages, :expected_messages, :produce_opts],
        ["dt-producer-topic1", 1, [{"key1", "value1"}, {"key2", "value2"}], :same, []],
        [
          "dt-producer-topic2",
          2,
          [{"key1", "value1"}, {"key2", "value2"}],
          :same,
          [partition: 1]
        ],
        ["dt-producer-topic3", 1, "this is the message", [{"", "this is the message"}], []],
        [
          "dt-producer-topic4",
          1,
          ["message1", "message2"],
          [{"", "message1"}, {"", "message2"}],
          []
        ],
        [
          "dt-producer-topic5",
          1,
          ["message1", "message2"],
          [{"", "message1"}, {"", "message2"}],
          [partition: 0]
        ]
      ]
    end

    test "produce can have custom headers" do
      topic = "headers-topic-1"
      connection = String.to_atom(topic)
      Elsa.create_topic(@brokers, topic)

      {:ok, supervisor} =
        Elsa.Supervisor.start_link(endpoints: @brokers, connection: connection, producer: [topic: topic])

      Elsa.Producer.ready?(connection)

      on_exit(fn -> assert_down(supervisor) end)

      messages = [
        %{key: "key1", value: "value1", headers: [{"header1", "one"}, {"header2", "two"}]},
        %{key: "key2", value: "value2", headers: [{"header2", "two"}]}
      ]

      Elsa.produce(connection, topic, messages, partition: 0)

      {:ok, _count, messages} = Elsa.fetch(@brokers, topic, partition: 0)
      assert [{"header1", "one"}, {"header2", "two"}] == Enum.at(messages, 0) |> Map.get(:headers)
      assert [{"header2", "two"}] == Enum.at(messages, 1) |> Map.get(:headers)
    end
  end

  describe "ad hoc produce_sync" do
    test "produces to the specified topic with no prior broker" do
      Elsa.create_topic(@brokers, "producer-topic3")

      iolist_message = [<<0, 0, 0, 0, 10>>, [[20], [[[[0], ['H', "id-token"]]]]]]
      Producer.produce(@brokers, "producer-topic3", [{"key1", "value1"}, {"key2", iolist_message}], partition: 0)

      parsed_messages = retrieve_results(@brokers, "producer-topic3", 0, 0)

      assert [{"key1", "value1"}, {"key2", IO.iodata_to_binary(iolist_message)}] == parsed_messages
    end
  end

  describe "partitioner functions" do
    test "produces to a topic partition randomly" do
      Elsa.create_topic(@brokers, "random-topic")
      connection = :elsa_test3

      {:ok, supervisor} =
        Elsa.Supervisor.start_link(endpoints: @brokers, connection: connection, producer: [topic: "random-topic"])

      on_exit(fn -> assert_down(supervisor) end)

      patient_produce(connection, "random-topic", [{"key1", "value1"}, {"key2", "value2"}],
        partitioner: Elsa.Partitioner.Random
      )

      parsed_messages = retrieve_results(@brokers, "random-topic", 0, 0)

      assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
    end

    test "producers to a topic partition based on an md5 hash of the key" do
      Elsa.create_topic(@brokers, "hashed-topic", partitions: 5)
      connection = :elsa_test4

      {:ok, supervisor} =
        Elsa.Supervisor.start_link(endpoints: @brokers, connection: connection, producer: [topic: "hashed-topic"])

      on_exit(fn -> assert_down(supervisor) end)

      patient_produce(connection, "hashed-topic", {"key", "value"}, partitioner: Elsa.Partitioner.Md5)

      parsed_messages = retrieve_results(@brokers, "hashed-topic", 1, 0)

      assert [{"key", "value"}] == parsed_messages
    end

    test "are backwards compatible with the old naming scheme" do
      topic = "old-default-topic"
      connection = :elsa_test_old_partitioner_name

      Elsa.create_topic(@brokers, topic)

      {:ok, supervisor} =
        Elsa.Supervisor.start_link(endpoints: @brokers, connection: connection, producer: [topic: topic])

      on_exit(fn -> assert_down(supervisor) end)
      patient_produce(connection, topic, {"key", "value"}, partitioner: :default)

      parsed_messages = retrieve_results(@brokers, topic, 0, 0)

      assert [{"key", "value"}] == parsed_messages
    end
  end

  describe "adding producers to an established connection" do
    test "produces messages without issue" do
      connection = :elsa_established_connection_test

      topic1 = "dynamic-producer-topic1"
      Elsa.create_topic(@brokers, topic1)

      topic2 = "dynamic-producer-topic2"
      Elsa.create_topic(@brokers, topic2)

      start_supervised(
        {Elsa.Supervisor,
         endpoints: @brokers,
         connection: connection,
         group_consumer: [
           group: "dynamic-producer-group",
           topics: [topic1],
           handler: Testing.SuperSimpleMessageHandler,
           config: [begin_offset: :earliest]
         ]}
      )

      Elsa.Supervisor.start_producer(connection, topic: topic1)

      patient_produce(connection, topic1, {"key1", "value1"}, [])

      messages = retrieve_results(@brokers, topic1, 0, 0)
      assert {"key1", "value1"} in messages

      Elsa.Supervisor.start_producer(connection, topic: topic2)

      patient_produce(connection, topic2, {"key2", "value2"}, [])

      messages = retrieve_results(@brokers, topic2, 0, 0)
      assert {"key2", "value2"} in messages
    end
  end

  describe "no producer started" do
    test "will return error when no client has been started" do
      Elsa.create_topic(@brokers, "bad-topic")

      messages = [{"key", "value"}]

      assert {:error, "Elsa with connection unknown_connection has not been started correctly"} ==
               Elsa.produce(:unknown_connection, "bad-topic", messages)
    end
  end

  defp patient_produce(connection, topic, messages, opts) do
    Patiently.wait_for!(
      fn ->
        case Producer.produce(connection, topic, messages, opts) do
          :ok ->
            true

          _ ->
            false
        end
      end,
      dwell: 100,
      max_retries: 50
    )
  end

  defp retrieve_results(endpoints, topic, partition, offset) do
    {:ok, {_count, messages}} = :brod.fetch(endpoints, topic, partition, offset)

    Enum.map(messages, fn msg -> {Elsa.Message.kafka_message(msg, :key), Elsa.Message.kafka_message(msg, :value)} end)
  end
end

defmodule Testing.SuperSimpleMessageHandler do
  use Elsa.Consumer.MessageHandler
end
