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

  describe "create_topic/3" do
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

  describe "delete_topic/2" do
    setup do
      Elsa.create_topic(@endpoints, "delete-topic1")
      Elsa.create_topic(@endpoints, "delete-topic2", partitions: 2)
      :ok
    end

    test "will delete a specified topic" do
      assert :ok == Elsa.delete_topic(@endpoints, "delete-topic1")

      topics = Elsa.list_topics(@endpoints)
      refute Enum.member?(topics, {"delete-topic1", 1})
    end

    test "will delete a topic with multiple partitions" do
      assert :ok == Elsa.delete_topic(@endpoints, "delete-topic2")

      topics = Elsa.list_topics(@endpoints)
      refute Enum.member?(topics, {"delete-topic2", 2})
    end
  end

  describe "produce/4" do
    test "will produce message to kafka topic" do
      Elsa.create_topic(@endpoints, "topic1")
      Elsa.produce(@endpoints, "topic1", {"key", "value1"})
      Elsa.produce(@endpoints, "topic1", [{"key2", "value2"}])

      {:ok, {_count, messages}} = :brod.fetch([{'localhost', 9092}], "topic1", 0, 0)

      parsed_messages = Enum.map(messages, fn msg -> Elsa.Message.new(msg, topic: "topic1", partition: 0) end)

      assert match?(
               [%Elsa.Message{key: "key", value: "value1"}, %Elsa.Message{key: "key2", value: "value2"}],
               parsed_messages
             )
    end
  end
end
