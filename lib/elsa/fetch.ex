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

    offset =
      Keyword.get_lazy(opts, :offset, fn ->
        {:ok, offset} = :brod.resolve_offset(endpoints, topic, partition, :earliest)
        offset
      end)

    case :brod.fetch(endpoints, topic, partition, offset) do
      {:ok, {partition_offset, messages}} ->
        transformed_messages = Enum.map(messages, &Elsa.Message.new(&1, topic: topic, partition: partition))
        {:ok, partition_offset, transformed_messages}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Retrieves all messages on a given topic across all partitions by default.
  Evaluates lazily, returning a `Stream` resource containing the messages.
  By default the starting offset is the earliest message offset and fetching
  continues until the latest offset at the time the stream is instantiated.
  Refine the scope of stream fetch by passing the `start_offset` and `end_offset`
  keyword arguments.
  """
  @spec fetch_stream(keyword(), String.t(), keyword()) :: Enumerable.t()
  def fetch_stream(endpoints, topic, opts \\ []) do
    partitions =
      case Keyword.get(opts, :partition) do
        nil ->
          0..(Elsa.Util.partition_count(endpoints, topic) - 1)

        partition ->
          [partition]
      end

    Enum.reduce(partitions, [], fn partition, acc ->
      partition_stream = fetch_partition_stream(endpoints, topic, partition, opts)
      [partition_stream | acc]
    end)
    |> Stream.concat()
  end

  @doc """
  Retrieves a stream of messages for which the supplied function evaluates
  to `true`. Search can be limited by an offset which is passed through to
  the underlying fetch_stream/3 call retrieving the messages to search.
  All options for fetch_stream/3 are respected.
  """
  @spec search(keyword(), String.t(), function(), keyword()) :: Enumerable.t()
  def search(endpoints, topic, search_function, opts \\ []) do
    all_messages = fetch_stream(endpoints, topic, opts)

    Stream.filter(all_messages, fn message ->
      search_function.(message)
    end)
  end

  @doc """
  Retrieves a stream of messages where the keys contains the supplied search
  string. Search can be further limited by an offset which is passed through to the
  underlying fetch_stream/3 call retrieving the messages to search. All options
  for fetch_stream/3 are respected.
  """
  @spec search_keys(keyword(), String.t(), String.t(), keyword()) :: Enumerable.t()
  def search_keys(endpoints, topic, search_term, opts \\ []) do
    search_by_keys = fn %Elsa.Message{key: key} -> String.contains?(key, search_term) end

    search(endpoints, topic, search_by_keys, opts)
  end

  @doc """
  Retrieves a stream of messages where the values contains the supplied search
  string. Search can be further limited by an offset which is passed through to the
  underlying fetch_stream/3 call retrieving the messages to search. All options
  for fetch_stream/3 are respected.
  """
  @spec search_values(keyword(), String.t(), String.t(), keyword()) :: Enumerable.t()
  def search_values(endpoints, topic, search_term, opts \\ []) do
    search_by_values = fn %Elsa.Message{value: value} -> String.contains?(value, search_term) end

    search(endpoints, topic, search_by_values, opts)
  end

  defp fetch_partition_stream(endpoints, topic, partition, opts) do
    Stream.resource(
      fn ->
        start_offset = retrieve_offset(opts, :start_offset, endpoints, topic, partition)
        end_offset = retrieve_offset(opts, :end_offset, endpoints, topic, partition)

        {start_offset, end_offset}
      end,
      fn {current_offset, end_offset} ->
        case current_offset < end_offset do
          true ->
            {:ok, _offset, messages} = fetch(endpoints, topic, partition: partition, offset: current_offset)
            next_offset = current_offset + Enum.count(messages)
            {messages, {next_offset, end_offset}}

          false ->
            {:halt, {current_offset, end_offset}}
        end
      end,
      fn offset -> offset end
    )
  end

  defp retrieve_offset(opts, :start_offset, endpoints, topic, partition) do
    Keyword.get_lazy(opts, :start_offset, fn ->
      {:ok, start_offset} = :brod.resolve_offset(endpoints, topic, partition, :earliest)
      start_offset
    end)
  end

  defp retrieve_offset(opts, :end_offset, endpoints, topic, partition) do
    Keyword.get_lazy(opts, :end_offset, fn ->
      {:ok, end_offset} = :brod.resolve_offset(endpoints, topic, partition, :latest)
      end_offset
    end)
  end
end
