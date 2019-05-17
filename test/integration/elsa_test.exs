defmodule ElsaTest do
  use ExUnit.Case
  use Divo

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
end
