defmodule Elsa.Partitioner.Default do
  @moduledoc """
  Defaults partition to 0.
  """

  @behaviour Elsa.Partitioner

  def partition(_count, _key), do: 0
end
