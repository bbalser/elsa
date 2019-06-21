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
    client = get_client(opts)

    Elsa.Producer.Manager.start_producer(endpoints, topic, name: client)
    produce_sync(topic, messages, Keyword.put(opts, :client, client))
  end

  def produce_sync(topic, messages, opts \\ [])

  def produce_sync(topic, messages, opts) when is_list(messages) do
    client = get_client(opts)

    case Keyword.get(opts, :partition) do
      nil ->
        messages
        |> Enum.map(&transform_message(&1, client, topic, opts))
        |> Enum.group_by(fn {partition, _} -> partition end, fn {_, message} -> message end)
        |> Enum.each(fn {partition, messages} -> do_produce_sync(client, topic, partition, messages) end)

      partition ->
        do_produce_sync(client, topic, partition, messages)
    end
  end

  def produce_sync(topic, {key, value}, opts) do
    client = get_client(opts)
    partition = get_partition(client, topic, key, opts)

    do_produce_sync(client, topic, partition, [{key, value}])
  end

  def produce_sync(topic, message, opts) do
    client = get_client(opts)
    partition = get_partition(client, topic, "", opts)

    do_produce_sync(client, topic, partition, [{"", message}])
  end

  defp transform_message({key, value}, client, topic, opts) do
    {get_partition(client, topic, key, opts), {key, value}}
  end

  defp transform_message(message, client, topic, opts) do
    {get_partition(client, topic, "", opts), {"", message}}
  end

  defp do_produce_sync(client, topic, partition, messages) do
    messages
    |> Util.chunk_by_byte_size()
    |> Enum.each(fn chunk -> :brod.produce_sync(client, topic, partition, "", chunk) end)
  end

  defp get_client(opts) do
    Keyword.get_lazy(opts, :client, &Elsa.default_client/0)
  end

  defp get_partition(client, topic, key, opts) do
    Keyword.get_lazy(opts, :partition, fn ->
      {:ok, partition_num} = :brod.get_partitions_count(client, topic)

      opts
      |> Keyword.get(:partitioner, :default)
      |> Elsa.Producer.Partitioner.partition(partition_num, key)
    end)
  end
end
