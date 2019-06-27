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
        unwrapped_messages = Enum.map(messages, &unwrap_messages(&1, partition))
        {:ok, partition_offset, unwrapped_messages}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Retrieves all messages on a given topic partition. Evaluates
  lazily, returning a `Stream` resource containing the messages.
  """
  @spec fetch_partition_stream(keyword(), String.t(), keyword()) :: Enumerable.t()
  def fetch_partition_stream(endpoints, topic, opts \\ []) do
    offset = Keyword.get(opts, :offset, 0)
    partition = Keyword.get(opts, :partition, 0)

    Stream.resource(
      fn -> offset end,
      fn offset ->
        case fetch(endpoints, topic, partition: partition, offset: offset) do
          {:ok, _partition_offset, messages} ->
            next_offset = offset + Enum.count(messages)
            {messages, next_offset}

          {:ok, {_, []}} ->
            {:halt, offset}

          {:error, _} ->
            {:halt, offset}
        end
      end,
      fn offset -> offset end
    )
  end

  @doc """
  Retrieves all messages on a given topic across all partitions.
  Evaluates lazily, returning a `Stream` resource containing the messages.
  """
  @spec fetch_topic_stream(keyword(), String.t(), keyword()) :: Enumerable.t()
  def fetch_topic_stream(endpoints, topic, opts \\ []) do
    offset = Keyword.get(opts, :offset, 0)
    partitions = Elsa.Util.partition_count(endpoints, topic) - 1

    Enum.reduce(0..partitions, [], fn partition, acc ->
      partition_stream = fetch_partition_stream(endpoints, topic, partition: partition, offset: offset)
      [partition_stream | acc]
    end)
    |> Stream.concat()
  end

  @doc """
  Retrieves an array of messages containing the supplied search string,
  sorted by time and with the partition and offset for reference. Search can
  be limited by an offset and time which are passed through to fetch_all/3 call
  retrieving the messages to search. By default, the search is applied against
  the message values but can be optionally switched to search on the message key
  by supplying the `search_by_key: true` option.
  """
  @spec search(keyword(), String.t(), String.t(), keyword()) :: Enumerable.t()
  def search(endpoints, topic, search_term, opts \\ []) do
    search_by = if Keyword.get(opts, :search_by_key), do: :key, else: :value
    all_messages = fetch_topic_stream(endpoints, topic, opts)

    Stream.filter(all_messages, fn message ->
      search_by(message, search_term, search_by)
    end)
  end

  defp unwrap_messages({_, offset, key, value, _, time, _}, partition), do: {partition, offset, key, value, time}

  defp search_by({_, _, _, value, _}, search_term, :value), do: search_term(value, search_term)

  defp search_by({_, _, key, _, _}, search_term, :key), do: search_term(key, search_term)

  defp search_term(term, search) do
    normalized_term = String.downcase(term)
    normalized_search = String.downcase(search)

    String.contains?(normalized_term, normalized_search)
  end
end
