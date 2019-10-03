defmodule Elsa.Group.DirectAcknowledgerTest do
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

  test "direct acknowledger ack over privately managed connection" do
    :ok = Elsa.create_topic(@endpoints, @topic)

    {:ok, elsa_sup_pid} =
      Elsa.Supervisor.start_link(
        connection: :test_direct_acker,
        endpoints: @endpoints,
        group_consumer: [
          group: @group,
          topics: [@topic],
          handler: MessageHandler,
          direct_ack: true,
          config: [
            begin_offset: :earliest
          ]
        ]
      )

    Elsa.produce(@endpoints, @topic, {"key1", "value1"}, partition: 0)

    Process.sleep(8_000)

    assert_async(fn ->
      assert 1 == get_committed_offsets(:test_direct_acker, @group, @topic, 0)
    end)

    Supervisor.stop(elsa_sup_pid)
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
