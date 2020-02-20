defmodule Elsa.Consumer do
  @moduledoc """
  Centralizes definition for common functions related
  to consumer interactions.
  """
  import Elsa.Supervisor, only: [registry: 1]

  @type offset :: integer() | String.t()

  @doc """
  Retrieve the process id of a consumer registered to the
  Elsa Registry and subscribes to it.
  """
  @spec subscribe(Elsa.connection(), Elsa.topic(), Elsa.partition(), term()) :: {:ok, pid()} | {:error, term()}
  def subscribe(connection, topic, partition, opts) do
    pid = get_consumer(connection, topic, partition)

    case :brod_consumer.subscribe(pid, self(), opts) do
      :ok -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec unsubscribe(Elsa.connection(), Elsa.topic(), Elsa.partition()) :: {:ok, pid} | {:error, term}
  def unsubscribe(connection, topic, partition) do
    pid = get_consumer(connection, topic, partition)

    :brod_consumer.unsubscribe(pid, self())
  end

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
