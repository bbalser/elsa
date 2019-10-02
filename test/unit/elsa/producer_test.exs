defmodule Elsa.ProducerTest do
  use ExUnit.Case
  use Placebo
  import TestHelper

  describe "produce_sync" do
    setup do
      {:ok, registry} = Elsa.Registry.start_link(name: :elsa_registry_test_client)
      Elsa.Registry.register_name({:elsa_registry_test_client, :brod_client}, self())

      on_exit(fn -> assert_down(registry) end)

      :ok
    end

    test "produce_sync fails with returning what messages did not get sent" do
      allow :brod_producer.produce(any(), any(), any()), seq: [:ok, {:error, :some_reason}, :ok]
      allow :brod_producer.sync_produce_request(any(), any()), return: {:ok, 0}
      Elsa.Registry.register_name({:elsa_registry_test_client, :"producer_topic-a_0"}, self())

      messages = Enum.map(1..2_000, fn i -> {"", random_string(i)} end)
      {:error, reason, failed} = Elsa.produce(:test_client, "topic-a", messages, connection: :test_client, partition: 0)

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
