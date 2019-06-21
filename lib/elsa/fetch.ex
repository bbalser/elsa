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
          time_messages = Enum.map(messages, &messages_with_time/1)
          [time_messages | acc]
        {:error, _} ->
          acc
      end
    end)
    |> List.flatten()
    |> Enum.filter(fn {timestamp, _, _} -> timestamp >= time end)
    |> Enum.sort(&(elem(&1, 2) <= elem(&2, 2)))
  end

  defp messages_with_offset({_, offset, key, value, _, _, _}), do: {offset, key, value}
  defp messages_with_time({_, _, key, value, _, time, _}), do: {time, key, value}
end
