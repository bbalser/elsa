defmodule Elsa.Group.CustomAcknowledgerTest do
  use ExUnit.Case
  use Divo
  import AsyncAssertion

  defmodule MessageHandler do
    use Elsa.Consumer.MessageHandler

    def handle_messages(_messages) do
      :ack
    end
  end

  @endpoints Application.get_env(:elsa, :brokers)
  @group "group-1a"
  @topic "topic-1a"

  test "custom acknowledger ack over privately managed connection" do
    :ok = Elsa.create_topic(@endpoints, @topic)

    {:ok, _elsa_sup_pid} =
      Elsa.Group.Supervisor.start_link(
        name: :test_custom_acker,
        endpoints: @endpoints,
        group: @group,
        topics: [@topic],
        handler: MessageHandler,
        custom_ack_hack: true,
        config: [
          begin_offset: :earliest
        ]
      )

    Elsa.produce(@endpoints, @topic, {"key1", "value1"}, partition: 0)

    Process.sleep(8_000)

    assert_async(fn ->
      assert 1 == get_committed_offsets(:test_custom_acker, @group, @topic, 0)
    end)
  end

  defp get_committed_offsets(client, group, topic, partition) do
    {:ok, responses} = :brod.fetch_committed_offsets(client, group)

    case Enum.find(responses, fn %{topic: t} -> topic == t end) do
      nil ->
        :undefined

      topic ->
        partition = Enum.find(topic.partition_responses, fn %{partition: p} -> partition == p end)
        partition.offset
    end
  end
end
