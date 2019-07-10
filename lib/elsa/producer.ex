defmodule Elsa.Producer do
  @moduledoc """
  Defines functions to write messages to topics based on either a list of endpoints or a named client.
  """

  alias Elsa.Util

  @doc """
  Write the supplied message(s) to the desired topic/partition via an endpoint list or named client.
  If no client or endpoint is supplied, the default named client is chosen. If no partition is supplied,
  the first (zero) partition is chosen. Value may be a single message or a list of messages. If a list
  of messages is supplied as the value, the key is defaulted to the binary "ignored" and understood
  to be ignored by the cluster.
  If message value is a list, it is expected to be a list of key/value tuples.
  Partition can be an integer corresponding to a specific numbered partition, or the atoms "random" or
  "md5". The atoms correspond to partitioner functions that will uniformely select a random partition
  from the total available partitions of the topic or assign an integer based on an md5 hash of the messages
  to be written respectively.
  """
  def produce(endpoints, topic, messages, opts \\ []) when is_list(endpoints) do
    name = get_client(opts)

    Elsa.Producer.Manager.start_producer(endpoints, topic, name: name)
    produce_sync(topic, messages, Keyword.put(opts, :name, name))
  end

  def produce_sync(topic, messages, opts \\ [])

  def produce_sync(topic, messages, opts) when is_list(messages) do
    name = get_client(opts)

    case Keyword.get(opts, :partition) do
      nil ->
        messages
        |> Enum.map(&transform_message(&1, name, topic, opts))
        |> Enum.group_by(fn {partition, _} -> partition end, fn {_, message} -> message end)
        |> Enum.each(fn {partition, messages} -> do_produce_sync(name, topic, partition, messages) end)

      partition ->
        do_produce_sync(name, topic, partition, messages)
    end
  end

  def produce_sync(topic, {key, value}, opts) do
    name = get_client(opts)
    partition = get_partition(name, topic, key, opts)

    do_produce_sync(name, topic, partition, [{key, value}])
  end

  def produce_sync(topic, message, opts) do
    name = get_client(opts)
    partition = get_partition(name, topic, "", opts)

    do_produce_sync(name, topic, partition, [{"", message}])
  end

  defp transform_message({key, value}, name, topic, opts) do
    {get_partition(name, topic, key, opts), {key, value}}
  end

  defp transform_message(message, name, topic, opts) do
    {get_partition(name, topic, "", opts), {"", message}}
  end

  defp do_produce_sync(name, topic, partition, messages) do
    messages
    |> Enum.map(&wrap_with_key/1)
    |> Util.chunk_by_byte_size()
    |> Enum.each(fn chunk -> :brod.produce_sync(name, topic, partition, "", chunk) end)
  end

  defp wrap_with_key({_key, _value} = message), do: message
  defp wrap_with_key(message), do: {"", message}

  defp get_client(opts) do
    Keyword.get_lazy(opts, :name, &Elsa.default_client/0)
  end

  defp get_partition(name, topic, key, opts) do
    Keyword.get_lazy(opts, :partition, fn ->
      {:ok, partition_num} = :brod.get_partitions_count(name, topic)

      opts
      |> Keyword.get(:partitioner, :default)
      |> Elsa.Producer.Partitioner.partition(partition_num, key)
    end)
  end
end
