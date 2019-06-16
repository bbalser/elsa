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

  def produce_sync(client, topic, partition, key, value) do
    partition_num =
      case partition do
        :random ->
          apply(Elsa.Producer.Partitioner, :random, [client, topic])
        :md5 ->
          apply(Elsa.Producer.Partitioner, :md5, [client, topic, key])
        partition ->
          partition
      end

    :brod.produce_sync(client, topic, partition_num, key, value)
  end
end
