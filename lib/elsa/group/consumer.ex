defmodule Elsa.Group.Consumer do
  import Elsa.Supervisor, only: [registry: 1]

  def subscribe(name, topic, partition, opts) do
    pid = get_consumer(name, topic, partition)

    case :brod_consumer.subscribe(pid, self(), opts) do
      :ok -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  def ack(name, topic, partition, offset) do
    pid = get_consumer(name, topic, partition)
    :brod_consumer.ack(pid, offset)
  end

  defp get_consumer(name, topic, partition) do
    consumer_name = :"consumer_#{topic}_#{partition}"
    Elsa.Registry.whereis_name({registry(name), consumer_name})
  end
end
