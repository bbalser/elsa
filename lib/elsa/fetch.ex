defmodule Elsa.Fetch do
  @moduledoc """
  Provides functions for doing one-off retrieval of
  messages from the Kafka cluster.
  """

  @doc """
  A simple interface for quickly retrieving a message set from
  the cluster on the given topic. Partition and offset may be
  specified as keyword options, defaulting to 0 in both cases if
  either is not supplied by the caller.
  """
  @spec fetch(keyword(), String.t(), keyword()) :: {:ok, integer(), [tuple()]} | {:error, term()}
  def fetch(endpoints, topic, opts \\ []) do
    partition = Keyword.get(opts, :partition, 0)
    offset = Keyword.get(opts, :offset, 0)

    case :brod.fetch(endpoints, topic, partition, offset) do
      {:ok, {partition_offset, messages}} ->
        offset_messages = Enum.map(messages, &messages_with_offset/1)
        {:ok, partition_offset, offset_messages}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Retrieves all messages on a given topic across all partitions,
  ordering by the timestamp attached to the message.
  """
  @spec fetch_all(keyword(), String.t(), keyword()) :: list()
  def fetch_all(endpoints, topic, opts \\ []) do
    offset = Keyword.get(opts, :offset, 0)
    time = Keyword.get(opts, :time, 0)
    partitions = Elsa.Util.partition_count(endpoints, topic) - 1

    Enum.reduce(0..partitions, [], fn partition, acc ->
      case :brod.fetch(endpoints, topic, partition, offset) do
        {:ok, {_, messages}} ->
          time_messages = Enum.map(messages, &messages_with_time(&1, partition))
          [time_messages | acc]

        {:error, _} ->
          acc
      end
    end)
    |> List.flatten()
    |> Enum.filter(fn {timestamp, _, _, _, _} -> timestamp >= time end)
    |> Enum.sort(&(elem(&1, 0) <= elem(&2, 0)))
  end

  @doc """
  Retrieves an array of messages containing the supplied search string,
  sorted by time and with the partition and offset for reference. Search can
  be limited by an offset and time which are passed through to fetch_all/3 call
  retrieving the messages to search. By default, the search is applied against
  the message values but can be optionally switched to search on the message key
  by supplying the `search_by_key: true` option.
  """
  @spec search(keyword(), String.t(), String.t(), keyword()) :: list()
  def search(endpoints, topic, search_term, opts \\ []) do
    search_by = if Keyword.get(opts, :search_by_key), do: :key, else: :value
    all_messages = fetch_all(endpoints, topic, opts)

    Enum.reduce(all_messages, [], fn message, acc ->
      case search_by(message, search_term, search_by) do
        true ->
          [message | acc]

        false ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  defp messages_with_offset({_, offset, key, value, _, _, _}), do: {offset, key, value}
  defp messages_with_time({_, offset, key, value, _, time, _}, partition), do: {time, partition, offset, key, value}

  defp search_by({_, _, _, _, value}, search_term, :value), do: search_term(value, search_term)

  defp search_by({_, _, _, key, _}, search_term, :key), do: search_term(key, search_term)

  defp search_term(term, search) do
    normalized_term = String.downcase(term)
    normalized_search = String.downcase(search)

    String.contains?(normalized_term, normalized_search)
  end
end
