defmodule ElsaTest do
  use ExUnit.Case
  use Divo
  require Elsa

  @endpoints [localhost: 9092]

  describe "list_topics/1" do
    test "will return topics given a client identifier" do
      topics = Elsa.list_topics(@endpoints)

      assert Enum.any?(topics, fn entry -> match?({"elsa-topic", 2}, entry) end)
    end
  end

  describe "create_topic/1" do
    test "will create a topic with 1 partition" do
      assert :ok == Elsa.create_topic(@endpoints, "new-topic")

      topics = Elsa.list_topics(@endpoints)
      assert Enum.any?(topics, fn entry -> match?({"new-topic", 1}, entry) end)
    end

    test "will create a topic with 2 partitions" do
      assert :ok == Elsa.create_topic(@endpoints, "new-topic-2", partitions: 2)

      topics = Elsa.list_topics(@endpoints)
      assert Enum.any?(topics, fn entry -> match?({"new-topic-2", 2}, entry) end)
    end
  end

  describe "produce_sync/5" do
    test "will produce message to kafka topic" do
      Elsa.create_topic(@endpoints, "topic1")
      Elsa.produce_sync(@endpoints, "topic1", 0, "key", "value1")
      Elsa.produce_sync(@endpoints, "topic1", 0, "key2", "value2")

      {:ok, {_count, messages}} = :brod.fetch([{'localhost', 9092}], "topic1", 0, 0)

      parsed_messages = Enum.map(messages, fn msg -> {Elsa.kafka_message(msg, :key), Elsa.kafka_message(msg, :value)} end)
      assert [{"key", "value1"}, {"key2", "value2"}] ==  parsed_messages
    end
  end
end
