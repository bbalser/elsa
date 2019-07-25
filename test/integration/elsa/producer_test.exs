defmodule Elsa.ProducerTest do
  use ExUnit.Case
  use Divo
  import Checkov

  alias Elsa.Producer
  alias Elsa.Producer.Supervisor, as: ProducerSupervisor
  require Elsa.Message

  @brokers [{'localhost', 9092}]

  describe "producer supervisors" do
    data_test "starts and stops the requested producer supervisor" do
      Elsa.create_topic(@brokers, topic, partitions: num_partitions)
      {:ok, _producer_sup} = ProducerSupervisor.start_link(name: topic, endpoints: @brokers, topic: topic)

      Producer.produce_sync(topic, messages, produce_opts)

      parsed_messages = retrieve_results(@brokers, topic, 0, 0) #Keyword.get(produce_opts, :partition, 0), 0)

      expected = if expected_messages == :same, do: messages, else: expected_messages

      assert expected == parsed_messages

      where [
        [:topic, :num_partitions, :messages, :expected_messages, :produce_opts],
        ["dt-producer-topic2", 2, [{"key1", "value1"}, {"key2", "value2"}], :same, [name: :"elsa_producer_client_dt-producer-topic2", partition: 1]],
        ["dt-producer-topic5", 1, ["message1", "message2"], [{"", "message1"}, {"", "message2"}], [name: :"elsa_producer_client_dt-producer-topic5", partition: 0]]
        # ["dt-producer-topic1", 1, [{"key1", "value1"}, {"key2", "value2"}], :same, [name: :"elsa_producer_client_dt-producer-topic1"]],
        # ["dt-producer-topic3", 1, "this is the message", [{"", "this is the message"}], [name: :"elsa_producer_client_dt-producer-topic3"]],
        # ["dt-producer-topic4", 1, ["message1", "message2"], [{"", "message1"}, {"", "message2"}], [name: :"elsa_producer_client_dt-producer-topic4"]],
      ]







    #   Manager.start_producer(@brokers, "elsa-topic", name: :elsa_client1)

    #   {:ok, partitions} = :brod.get_partitions_count(:elsa_client1, "elsa-topic")

    #   producer_pids =
    #     Enum.map(0..(partitions - 1), fn part -> :brod.get_producer(:elsa_client1, "elsa-topic", part) end)

    #   all_alive = Enum.map(producer_pids, fn {_, pid} -> Process.alive?(pid) end)
    #   assert Enum.all?(all_alive, fn alive -> alive == true end)

    #   Manager.stop_producer(:elsa_client1, "elsa-topic")

    #   all_stopped = Enum.map(producer_pids, fn {_, pid} -> Process.alive?(pid) end)
    #   assert Enum.all?(all_stopped, fn alive -> alive == false end)
    end
  end

  # describe "preconfigured broker" do
  #   data_test "produces to topic" do
  #     Elsa.create_topic(@brokers, topic, partitions: num_partitions)
  #     Manager.start_producer(@brokers, topic, producer_opts)

  #     Producer.produce_sync(topic, messages, produce_opts)

  #     parsed_messages = retrieve_results(@brokers, topic, Keyword.get(produce_opts, :partition, 0), 0)

  #     expected = if expected_messages == :same, do: messages, else: expected_messages

  #     assert expected == parsed_messages

  #     where [
  #       [:topic, :num_partitions, :messages, :expected_messages, :producer_opts, :produce_opts],
  #       ["dt-producer-topic1", 1, [{"key1", "value1"}, {"key2", "value2"}], :same, [], []],
  #       [
  #         "dt-producer-topic2",
  #         2,
  #         [{"key1", "value1"}, {"key2", "value2"}],
  #         :same,
  #         [name: :elsa_client2],
  #         [name: :elsa_client2, partition: 1]
  #       ],
  #       ["dt-producer-topic3", 1, "this is the message", [{"", "this is the message"}], [], []],
  #       ["dt-producer-topic4", 1, ["message1", "message2"], [{"", "message1"}, {"", "message2"}], [], []],
  #       ["dt-producer-topic5", 1, ["message1", "message2"], [{"", "message1"}, {"", "message2"}], [], [partition: 0]]
  #     ]
  #   end
  # end

  # describe "ad hoc produce_sync" do
  #   test "produces to the specified topic with no prior broker" do
  #     Elsa.create_topic(@brokers, "producer-topic3")

  #     Producer.produce(@brokers, "producer-topic3", [{"key1", "value1"}, {"key2", "value2"}], partition: 0)

  #     parsed_messages = retrieve_results(@brokers, "producer-topic3", 0, 0)

  #     assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
  #   end
  # end

  # describe "partitioner functions" do
  #   test "produces to a topic partition randomly" do
  #     Elsa.create_topic(@brokers, "random-topic")

  #     Manager.start_producer(@brokers, "random-topic", name: :elsa_client3)

  #     Producer.produce_sync("random-topic", [{"key1", "value1"}, {"key2", "value2"}],
  #       name: :elsa_client3,
  #       partitioner: :random
  #     )

  #     parsed_messages = retrieve_results(@brokers, "random-topic", 0, 0)

  #     assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
  #   end

  #   test "producers to a topic partition based on an md5 hash of the key" do
  #     Elsa.create_topic(@brokers, "hashed-topic", partitions: 5)

  #     Manager.start_producer(@brokers, "hashed-topic", name: :elsa_client4)

  #     Producer.produce_sync("hashed-topic", {"key", "value"}, name: :elsa_client4, partitioner: :md5)

  #     parsed_messages = retrieve_results(@brokers, "hashed-topic", 1, 0)

  #     assert [{"key", "value"}] == parsed_messages
  #   end
  # end

  # describe "no producer started" do
  #   test "will return {:error, :client_down when no client has been started}" do
  #     Elsa.create_topic(@brokers, "bad-topic")

  #     messages = [{"key", "value"}]

  #     assert {:error, :client_down} == Elsa.produce_sync("bad-topic", messages)
  #   end
  # end

  defp retrieve_results(endpoints, topic, partition, offset) do
    {:ok, {_count, messages}} = :brod.fetch(endpoints, topic, partition, offset)

    Enum.map(messages, fn msg -> {Elsa.Message.kafka_message(msg, :key), Elsa.Message.kafka_message(msg, :value)} end)
  end
end
