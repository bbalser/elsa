defmodule Elsa.Consumer do
  @moduledoc """
  Public api to consumer acks asynchronously for simple consumers
  """

  import Elsa.Supervisor, only: [registry: 1]

  @type offset :: non_neg_integer()

  @doc """
  Retrieves a process id of a consumer registered to the
  Elsa Registry and performs a consume-ack of the messages
  ready to be read off the topic.
  """
  @spec ack(Elsa.connection(), Elsa.topic(), Elsa.partition(), offset()) :: :ok
  def ack(connection, topic, partition, offset) do
    pid = get_consumer(connection, topic, partition)
    :brod_consumer.ack(pid, offset)
  end

  defp get_consumer(connection, topic, partition) do
    consumer_name = :"consumer_#{topic}_#{partition}"
    Elsa.Registry.whereis_name({registry(connection), consumer_name})
  end
end
