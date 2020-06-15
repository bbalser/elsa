defmodule Elsa.Partitioner do
  @moduledoc """
  Behaviour for partitioning messages produced to Kafka.
  """

  @typedoc """
  The number of partitions for a topic.
  """
  @type partition_count :: pos_integer

  @typedoc """
  Partition selected by the partitioning strategy.
  """
  @type partition :: non_neg_integer

  @callback partition(partition_count, key :: term) :: partition
end
