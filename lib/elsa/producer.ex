defmodule Elsa.Producer do
  @moduledoc """
  Defines functions to send messages to topics based on either a list of endpoints or a named client.
  """
  def produce_sync(client \\ Elsa.default_client(), topic, partition \\ 0, key \\ "ignored", value)

  def produce_sync(endpoints, topic, partition, key, value) when is_list(endpoints) do
    client = Elsa.default_client()

    Elsa.Producer.Supervisor.start_producer(endpoints, topic, name: client)
    produce_sync(client, topic, partition, key, value)
  end

  def produce_sync(client, topic, partitioner, key, value) do
    partition =
      if is_atom(partitioner) do
        {:ok, partition_num} = :brod.get_partitions_count(client, topic)
        apply(Elsa.Producer.Partitioner, partitioner, [partition_num, value])
      else
        partitioner
      end

    :brod.produce_sync(client, topic, partition, key, value)
  end
end
