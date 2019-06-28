defmodule Elsa.FetchTest do
  use ExUnit.Case
  use Divo

  @endpoints [localhost: 9092]

  setup_all do
    Elsa.create_topic(@endpoints, "fetch-tests", partitions: 3)

    Enum.map([0, 1, 2], fn partition ->
      messages = Enum.map(1..10, &construct_message(&1, partition))
      Elsa.produce(@endpoints, "fetch-tests", messages, partition: partition)
    end)

    :ok
  end

  describe "fetch/3" do
    test "fetches from the default offset and partition" do
      {:ok, offset, messages} = Elsa.fetch(@endpoints, "fetch-tests")
      result = Enum.map(messages, fn {_partition, _offset, _key, value, _timestamp} -> value end)

      assert 10 == offset
      assert ["val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9", "val10"] == result
    end

    test "fetches from the specified offset and partition" do
      {:ok, offset, messages} = Elsa.fetch(@endpoints, "fetch-tests", partition: 1, offset: 3)
      result = Enum.map(messages, fn {_partition, _offset, _key, value, _timestamp} -> value end)

      assert 10 == offset
      assert ["val14", "val15", "val16", "val17", "val18", "val19", "val20"] == result
    end
  end

  describe "fetch_stream/3" do
    test "fetches all messages from the specified partition" do
      result =
        Elsa.Fetch.fetch_stream(@endpoints, "fetch-tests", partition: 2)
        |> Enum.map(fn {_partition, _offset, _key, value, _timestamp} -> value end)
        |> Enum.sort()

      expected = Enum.map(21..30, fn num -> "val#{num}" end) |> Enum.sort()

      assert expected == result
    end

    test "fetches all messages from the topic across all partitions" do
      result =
        Elsa.Fetch.fetch_stream(@endpoints, "fetch-tests")
        |> Enum.map(fn {_partition, _offset, _key, value, _timestamp} -> value end)
        |> Enum.sort()

      expected = Enum.map(1..30, fn num -> "val#{num}" end) |> Enum.sort()

      assert expected == result
    end

    test "fetches all messages across partitions since the specified offset" do
      result =
        Elsa.Fetch.fetch_stream(@endpoints, "fetch-tests", start_offset: 9)
        |> Enum.map(fn {_partition, _offset, _key, value, _timestamp} -> value end)
        |> Enum.sort()

      expected = ["val10", "val20", "val30"]

      assert expected == result
    end
  end

  describe "search/4" do
    test "finds specified message by value" do
      message = Elsa.Fetch.search(@endpoints, "fetch-tests", "val19")
      result = Enum.map(message, fn {_, _, key, value, _} -> {key, value} end)

      assert [{"key19", "val19"}] == result
    end

    test "finds specified message by key" do
      messages = Elsa.Fetch.search(@endpoints, "fetch-tests", "key2", search_by_key: true)
      result = Enum.map(messages, fn {_, _, key, value, _} -> {key, value} end)

      expected = [{"key2", "val2"} | Enum.map(20..29, fn num -> {"key#{num}", "val#{num}"} end)]

      for message <- result, do: assert(message in expected)
    end

    test "returns empty list when no match is found" do
      value_result = Elsa.Fetch.search(@endpoints, "fetch-tests", "foo") |> Enum.to_list()
      key_result = Elsa.Fetch.search(@endpoints, "fetch-tests", "bar", search_by_key: true) |> Enum.to_list()

      assert value_result == []
      assert key_result == []
    end
  end

  defp construct_message(num, multiplier) do
    index = num + 10 * multiplier
    {"key#{index}", "val#{index}"}
  end
end
