defmodule Elsa.Producer.Partitioner do
  def random(client, topic) do
    partitions = partition_count(client, topic)

    :crypto.rand_uniform(0, partitions)
  end

  def md5(client, topic, key) do
    partitions = partition_count(client, topic)

    :crypto.hash(:md5, key)
    |> :binary.bin_to_list()
    |> Enum.sum()
    |> rem(partitions)
  end

  defp partition_count(client, topic) do
    {:ok, partitions} = :brod.get_partitions_count(client, topic)

    partitions
  end
end
