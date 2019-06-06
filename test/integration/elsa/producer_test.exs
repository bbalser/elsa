defmodule Elsa.ProducerTest do
  use ExUnit.Case
  use Divo
  alias Elsa.Producer
  require Elsa

  @brokers [{'localhost', 9092}]

  describe "preconfigured broker" do
    test "produces to topic with default client and partition" do
      Elsa.create_topic(@brokers, "producer-topic1")
      Elsa.Producer.start_producer(@brokers, "producer-topic1")

      Producer.produce_sync("producer-topic1", "ignored", [{"key1", "value1"}, {"key2", "value2"}])

      parsed_messages = retrieve_results(@brokers, "producer-topic1", 0, 0)

      assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
    end

    test "produces to topic with named client and partition" do
      Elsa.create_topic(@brokers, "producer-topic2", partitions: 2)
      Elsa.Producer.start_producer(@brokers, "producer-topic2", name: :elsa_client)

      Producer.produce_sync(:elsa_client, "producer-topic2", 1, "ignored", [{"key1", "value1"}, {"key2", "value2"}])

      parsed_messages = retrieve_results(@brokers, "producer-topic2", 1, 0)

      assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
    end
  end

  describe "ad hoc produce_sync" do
    test "produces to the specified topic with no prior broker" do
      Elsa.create_topic(@brokers, "producer-topic3")

      Elsa.produce_sync(@brokers, "producer-topic3", 0, "ignored", [{"key1", "value1"}, {"key2", "value2"}])

      parsed_messages = retrieve_results(@brokers, "producer-topic3", 0, 0)

      assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
    end
  end

  defp retrieve_results(endpoints, topic, partition, offset) do
    {:ok, {_count, messages}} = :brod.fetch(endpoints, topic, partition, offset)

    Enum.map(messages, fn msg -> {Elsa.kafka_message(msg, :key), Elsa.kafka_message(msg, :value)} end)
  end
end
