defmodule Elsa.Partitioner.Md5Test do
  use ExUnit.Case

  describe "partition/2" do
    test "returns a predictable number based on the hash of the key" do
      assert Elsa.Partitioner.Md5.partition(2, "key1") == 1
    end
  end
end
