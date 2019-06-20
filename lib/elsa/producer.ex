defmodule Elsa.Producer do
  @moduledoc """
  Defines functions to write messages to topics based on either a list of endpoints or a named client.
  """

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
  @spec produce_sync(
          keyword() | atom(),
          String.t(),
          integer() | atom(),
          String.t(),
          String.t() | [{String.t(), String.t()}]
        ) ::
          :ok | no_return()
  def produce_sync(client \\ Elsa.default_client(), topic, partition \\ 0, key \\ "ignored", value)

  def produce_sync(endpoints, topic, partition, key, value) when is_list(endpoints) do
    client = Elsa.default_client()

    Elsa.Producer.Manager.start_producer(endpoints, topic, name: client)
    produce_sync(client, topic, partition, key, value)
  end

  def produce_sync(client, topic, partitioner, key, value) when is_atom(partitioner) do
    {:ok, partition_num} = :brod.get_partitions_count(client, topic)
    partition = Elsa.Producer.Partitioner.partition(partitioner, partition_num, value)
    produce_sync(client, topic, partition, key, value)
  end

  def produce_sync(client, topic, partition, key, value) do
    :brod.produce_sync(client, topic, partition, key, value)
  end
end
