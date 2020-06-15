defmodule Elsa.Partitioner.Md5 do
  @moduledoc """
  Chooses a topic partition based on an md5 hash of the
  supplied value, typically the message content.
  """

  @behaviour Elsa.Partitioner

  def partition(count, key) do
    :crypto.hash(:md5, key)
    |> :binary.bin_to_list()
    |> Enum.sum()
    |> rem(count)
  end
end
