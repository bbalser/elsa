defmodule Elsa.PartitionerTest do
  use ExUnit.Case

  describe "md5/3 partitioner" do
    test "returns a predictable number based on the hash of the key" do
      partition = Elsa.Producer.Partitioner.md5( 2, "key1")
      assert partition == 1
    end
  end
end
