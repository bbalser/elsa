defmodule Elsa.Partitioner.Random do
  @moduledoc """
  Randomly chooses a topic partition uniformly from the available
  number of partitions.
  """

  @behaviour Elsa.Partitioner

  def partition(count, _key) do
    :rand.uniform(count) - 1
  end
end
