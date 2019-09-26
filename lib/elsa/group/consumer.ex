defmodule Elsa.Group.Consumer do
  @moduledoc """
  Centralizes definition for common functions related
  to consumer interactions.
  """
  import Elsa.Supervisor, only: [registry: 1]

  @type name :: atom() | String.t()
  @type offset :: integer() | String.t()

  @doc """
  Retrieve the process id of a consumer registered to the
  Elsa Registry and subscribes to it.
  """
  @spec subscribe(name(), Elsa.topic(), Elsa.partition(), term()) :: {:ok, pid()} | {:error, term()}
  def subscribe(name, topic, partition, opts) do
    pid = get_consumer(name, topic, partition)

    case :brod_consumer.subscribe(pid, self(), opts) do
      :ok -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Retrieves a process id of a consumer registered to the
  Elsa Registry and performs a consume-ack of the messages
  ready to be read off the topic.
  """
  @spec ack(name(), Elsa.topic(), Elsa.partition(), offset()) :: :ok
  def ack(name, topic, partition, offset) do
    pid = get_consumer(name, topic, partition)
    :brod_consumer.ack(pid, offset)
  end

  defp get_consumer(name, topic, partition) do
    consumer_name = :"consumer_#{topic}_#{partition}"
    Elsa.Registry.whereis_name({registry(name), consumer_name})
  end
end
