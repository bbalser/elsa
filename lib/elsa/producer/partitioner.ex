defmodule Elsa.Producer.Partitioner do
  @moduledoc """
  Provides functions for automatically selecting the
  topic partition to write a message.
  """

  @doc """
  Randomly choose a topic partition uniformely from
  the available total number of partitions.
  """
  @spec partition(:random, integer(), term() :: integer()
  def partition(:random, partition_count, _key) do
    :crypto.rand_uniform(0, partition_count)
  end

  @doc """
  Choose a topic partition based on an md5 hash of
  the supplied value, typically the message content.
  """
  @spec partition(:md5, integer(), term() :: integer()
  def partition(:md5, partition_count, key) do
    :crypto.hash(:md5, key)
    |> :binary.bin_to_list()
    |> Enum.sum()
    |> rem(partition_count)
  end
end
