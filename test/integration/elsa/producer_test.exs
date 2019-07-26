defmodule Elsa.ProducerTest do
  use ExUnit.Case
  use Divo
  import Checkov

  alias Elsa.Producer
  alias Elsa.Producer.Supervisor, as: ProducerSupervisor
  require Elsa.Message

  @brokers [{'localhost', 9092}]

  describe "producer supervisors" do
    test "starts and stops the requested producer supervisor" do
      topic = "elsa-producer-topic0"
      Elsa.create_topic(@brokers, topic)
      {:ok, producer_sup} = ProducerSupervisor.start_link(name: :elsa_producer_test1, endpoints: @brokers, topic: topic)

      children = Enum.map(Supervisor.which_children(producer_sup), fn {_, pid, _, _} -> pid end)
      Process.exit(producer_sup, :shutdown)

      assert Enum.all?(children, fn child -> Process.alive?(child) == false end) == false
    end
  end

  describe "preconfigured broker" do
    data_test "produces to topic" do
      Elsa.create_topic(@brokers, topic, partitions: num_partitions)
      ProducerSupervisor.start_link(name: String.to_atom(topic), topic: topic, endpoints: @brokers)

      patient_produce(topic, messages, produce_opts)

      parsed_messages = retrieve_results(@brokers, topic, Keyword.get(produce_opts, :partition, 0), 0)

      expected = if expected_messages == :same, do: messages, else: expected_messages

      assert expected == parsed_messages

      where [
        [:topic, :num_partitions, :messages, :expected_messages, :produce_opts],
        ["dt-producer-topic1", 1, [{"key1", "value1"}, {"key2", "value2"}], :same, [name: :"dt-producer-topic1"]],
        [
          "dt-producer-topic2",
          2,
          [{"key1", "value1"}, {"key2", "value2"}],
          :same,
          [name: :"dt-producer-topic2", partition: 1]
        ],
        ["dt-producer-topic3", 1, "this is the message", [{"", "this is the message"}], [name: :"dt-producer-topic3"]],
        [
          "dt-producer-topic4",
          1,
          ["message1", "message2"],
          [{"", "message1"}, {"", "message2"}],
          [name: :"dt-producer-topic4"]
        ],
        [
          "dt-producer-topic5",
          1,
          ["message1", "message2"],
          [{"", "message1"}, {"", "message2"}],
          [name: :"dt-producer-topic5", partition: 0]
        ]
      ]
    end
  end

  describe "ad hoc produce_sync" do
    test "produces to the specified topic with no prior broker" do
      Elsa.create_topic(@brokers, "producer-topic3")

      Patiently.wait_for!(
        fn ->
          case Producer.produce(@brokers, "producer-topic3", [{"key1", "value1"}, {"key2", "value2"}], partition: 0) do
            :ok ->
              true
            _ ->
              false
          end
        end,
        dwell: 100,
        max_retries: 5
      )

      parsed_messages = retrieve_results(@brokers, "producer-topic3", 0, 0)

      assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
    end
  end

  describe "partitioner functions" do
    test "produces to a topic partition randomly" do
      Elsa.create_topic(@brokers, "random-topic")

      ProducerSupervisor.start_link(name: :elsa_test3, topic: "random-topic", endpoints: @brokers)

      patient_produce("random-topic", [{"key1", "value1"}, {"key2", "value2"}], name: :elsa_test3, partitioner: :random)

      parsed_messages = retrieve_results(@brokers, "random-topic", 0, 0)

      assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
    end

    test "producers to a topic partition based on an md5 hash of the key" do
      Elsa.create_topic(@brokers, "hashed-topic", partitions: 5)

      ProducerSupervisor.start_link(name: :elsa_test4, topic: "hashed-topic", endpoints: @brokers)

      patient_produce("hashed-topic", {"key", "value"}, name: :elsa_test4, partitioner: :md5)

      parsed_messages = retrieve_results(@brokers, "hashed-topic", 1, 0)

      assert [{"key", "value"}] == parsed_messages
    end
  end

  describe "no producer started" do
    test "will return {:error, :client_down when no client has been started}" do
      Elsa.create_topic(@brokers, "bad-topic")

      messages = [{"key", "value"}]

      assert {:error, :client_down} == Elsa.produce_sync("bad-topic", messages)
    end
  end

  defp patient_produce(topic, messages, opts) do
    Patiently.wait_for!(
      fn ->
        case Producer.produce_sync(topic, messages, opts) do
          :ok ->
            true

          _ ->
            false
        end
      end,
      dwell: 100,
      max_retries: 5
    )
  end

  defp retrieve_results(endpoints, topic, partition, offset) do
    {:ok, {_count, messages}} = :brod.fetch(endpoints, topic, partition, offset)

    Enum.map(messages, fn msg -> {Elsa.Message.kafka_message(msg, :key), Elsa.Message.kafka_message(msg, :value)} end)
  end
end
