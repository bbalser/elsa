defmodule Elsa.FetchTest do
  use ExUnit.Case
  use Divo

  @endpoints [localhost: 9092]

  describe "fetch/3" do
    test "fetches from the default offset and partition" do
      Elsa.create_topic(@endpoints, "fetch1")
      Elsa.produce(@endpoints, "fetch1", [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}])

      {:ok, offset, messages} = Elsa.fetch(@endpoints, "fetch1")
      result = Enum.map(messages, fn {_partition, _offset, _key, value, _timestamp} -> value end)

      assert 3 == offset
      assert 3 == Enum.count(messages)
      assert ["value1", "value2", "value3"] == result
    end

    test "fetches from the specified offset and partition" do
      Elsa.create_topic(@endpoints, "fetch2", partitions: 2)
      Elsa.produce(@endpoints, "fetch2", [{"key1", "value1"}, {"key2", "value2"}])
      Elsa.produce(@endpoints, "fetch2", [{"key3", "value3"}, {"key4", "value4"}], partition: 1)

      {:ok, offset, messages} = Elsa.fetch(@endpoints, "fetch2", partition: 1)
      result = Enum.map(messages, fn {_partition, _offset, _key, value, _timestamp} -> value end)

      assert 2 == offset
      assert 2 == Enum.count(messages)
      assert ["value3", "value4"] == result
    end
  end

  describe "fetch_topic_stream/3" do
    test "fetches all messages from the topic" do
      Elsa.create_topic(@endpoints, "fetch3", partitions: 2)
      Elsa.produce(@endpoints, "fetch3", [{"key1", "value1"}, {"key2", "value2"}], partition: 0)
      Elsa.produce(@endpoints, "fetch3", [{"key3", "value3"}, {"key4", "value4"}], partition: 1)

      messages = Elsa.Fetch.fetch_partition_stream(@endpoints, "fetch3") |> Enum.to_list()
      result = Enum.map(messages, fn {_partition, _offset, _key, value, _timestamp} -> value end)

      assert 4 == Enum.count(messages)
      assert ["value1", "value2", "value3", "value4"] == result
    end
  end

  # describe "search/4" do
  #   test "finds specified message by value" do
  #     Elsa.create_topic(@endpoints, "search1", partitions: 2)

  #     Elsa.produce(@endpoints, "search1", [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}],
  #       partitioner: :random
  #     )

  #     message = Elsa.Fetch.search(@endpoints, "search1", "value2")
  #     result = Enum.map(message, fn {_, _, _, key, value} -> {key, value} end)

  #     assert [{"key2", "value2"}] == result
  #   end

  #   test "finds specified message by key" do
  #     Elsa.create_topic(@endpoints, "search2", partitions: 2)

  #     Elsa.produce(@endpoints, "search2", [{"key1", "value1"}, {"foo", "value2"}, {"key3", "value3"}],
  #       partitioner: :random
  #     )

  #     messages = Elsa.Fetch.search(@endpoints, "search2", "key", search_by_key: true)
  #     result = Enum.map(messages, fn {_, _, _, key, value} -> {key, value} end)

  #     for message <- result, do: assert(message in [{"key1", "value1"}, {"key3", "value3"}])
  #   end

  #   test "finds specified message filtered by time" do
  #     Elsa.create_topic(@endpoints, "search3", partitions: 2)
  #     Elsa.produce(@endpoints, "search3", [{"key1", "value1"}, {"key2", "value2"}], partitioner: :random)

  #     time_marker = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

  #     Elsa.produce(@endpoints, "search3", [{"key3", "value3"}, {"key4", "value4"}], partitioner: :random)

  #     messages = Elsa.Fetch.search(@endpoints, "search3", "key", time: time_marker, search_by_key: true)
  #     result = Enum.map(messages, fn {_, _, _, key, value} -> {key, value} end)

  #     for message <- result, do: assert(message in [{"key3", "value3"}, {"key4", "value4"}])
  #   end

  #   test "returns empty list when no match is found" do
  #     Elsa.create_topic(@endpoints, "search4")
  #     Elsa.produce(@endpoints, "search4", [{"key", "value"}])

  #     value_result = Elsa.Fetch.search(@endpoints, "search4", "foo")
  #     key_result = Elsa.Fetch.search(@endpoints, "search4", "bar")

  #     assert value_result == []
  #     assert key_result == []
  #   end
  # end
end
