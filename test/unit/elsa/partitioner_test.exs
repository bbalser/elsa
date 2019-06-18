defmodule Elsa.PartitionerTest do
  use ExUnit.Case

  describe "partition/3 as md5 partitioner" do
    test "returns a predictable number based on the hash of the key" do
      partition = Elsa.Producer.Partitioner.partition(:md5, 2, "key1")
      assert partition == 1
    end
  end
end
