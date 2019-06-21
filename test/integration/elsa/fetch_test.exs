defmodule Elsa.FetchTest do
  use ExUnit.Case
  use Divo

  @endpoints [localhost: 9092]

  describe "fetch/3" do
    test "fetches from the default offset and partition" do
      Elsa.create_topic(@endpoints, "fetch1")
      Elsa.produce(@endpoints, "fetch1", [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}])

      {:ok, offset, messages} = Elsa.fetch(@endpoints, "fetch1")
      values = Enum.map(messages, fn {_offset, _key, value} -> value end)

      assert 3 == offset
      assert 3 == Enum.count(messages)
      assert ["value1", "value2", "value3"] == values
    end

    test "fetches from the specified offset and partition" do
      Elsa.create_topic(@endpoints, "fetch2", partitions: 2)
      Elsa.produce(@endpoints, "fetch2", [{"key1", "value1"}, {"key2", "value2"}])
      Elsa.produce(@endpoints, "fetch2", [{"key3", "value3"}, {"key4", "value4"}], partition: 1)

      {:ok, offset, messages} = Elsa.fetch(@endpoints, "fetch2", partition: 1)
      values = Enum.map(messages, fn {_offset, _key, value} -> value end)

      assert 2 == offset
      assert 2 == Enum.count(messages)
      assert ["value3", "value4"] == values
    end
  end

  describe "fetch_all/3" do
    test "fetches all messages from the topic" do
      Elsa.create_topic(@endpoints, "fetch3", partitions: 2)
      Elsa.produce(@endpoints, "fetch3", [{"key1", "value1"}, {"key2", "value2"}], partition: 0)
      Elsa.produce(@endpoints, "fetch3", [{"key3", "value3"}, {"key4", "value4"}], partition: 1)

      messages = Elsa.Fetch.fetch_all(@endpoints, "fetch3")
      values = Enum.map(messages, fn {_timestamp, _key, value} -> value end)

      assert 4 == Enum.count(messages)
      assert ["value1", "value2", "value3", "value4"] == values
    end

    test "fetches all messages from the topic after the time offset" do
      Elsa.create_topic(@endpoints, "fetch4", partitions: 2)
      Elsa.produce(@endpoints, "fetch4", [{"key1", "value1"}], partition: 0)
      Elsa.produce(@endpoints, "fetch4", [{"key2", "value2"}], partition: 1)

      time_marker = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      Elsa.produce(@endpoints, "fetch4", [{"key3", "value3"}], partition: 0)
      Elsa.produce(@endpoints, "fetch4", [{"key4", "value4"}], partition: 1)

      messages = Elsa.Fetch.fetch_all(@endpoints, "fetch4", time: time_marker)
      values = Enum.map(messages, fn {_timestamp, _key, value} -> value end)

      assert 2 == Enum.count(messages)
      assert ["value3", "value4"] == values
    end
  end
end
