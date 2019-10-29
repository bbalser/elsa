defmodule Elsa.Consumer.WorkerTest do
  use ExUnit.Case
  use Divo

  @endpoints Application.get_env(:elsa, :brokers)
  @topic "my-topic"

  test "simply consumes messages from configured topic/partition" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, @topic)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, _} =
      Elsa.Supervisor.start_link(
        connection: :test_simple_consumer,
        endpoints: @endpoints,
        consumer: [
          topic: @topic,
          partition: 0,
          begin_offset: :earliest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    Elsa.produce(@endpoints, @topic, {"key1", "value1"}, partition: 0)
    assert_receive [%Elsa.Message{value: "value1"}], 5_000
  end
end

defmodule MyMessageHandler do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages, state) do
    send(state[:pid], messages)
    :ack
  end
end
