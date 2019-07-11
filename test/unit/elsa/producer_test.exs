defmodule Elsa.ProducerTest do
  use ExUnit.Case
  use Placebo

  describe "produce_sync" do
    test "stuff" do
      allow Elsa.Util.client?(any()), return: true, meck_options: [:passthrough]
      allow :brod.produce_sync(any(), any(), any(), any(), any()), seq: [:ok, {:error, :some_reason}, :ok]

      messages = Enum.map(1..2_000, fn i -> {"", random_string(i)} end)
      {:error, reason, failed} = Elsa.produce_sync("topic-a", messages, name: :test_client, partition: 0)

      assert reason == "1331 messages succeeded before elsa producer failed midway through due to :some_reason"
      assert 2000 - 1331 == length(failed)
      assert failed == Enum.drop(messages, 1331)
    end
  end

  defp random_string(length) do
    :crypto.strong_rand_bytes(length)
    |> Base.encode64()
    |> binary_part(0, length)
  end
end
