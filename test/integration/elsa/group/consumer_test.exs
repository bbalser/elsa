defmodule Elsa.Group.ConsumerTest do
  use ExUnit.Case
  use Divo
  import AsyncAssertion
  require Logger

  @brokers Application.get_env(:elsa, :brokers)

  test "Elsa.Consumer will hand messages to the handler with state" do
    {:ok, pid} =
      Elsa.Group.Supervisor.start_link(
        name: :name1,
        endpoints: @brokers,
        group: "group1",
        topics: ["elsa-topic"],
        handler: Testing.ExampleMessageHandlerWithState,
        handler_init_args: %{pid: self()},
        config: [begin_offset: :earliest]
      )

    send_messages(["message1", "message2"])

    assert_receive {:message, %{topic: "elsa-topic", partition: 0, offset: _, key: "", value: "message1"}}, 5_000
    assert_receive {:message, %{topic: "elsa-topic", partition: 1, offset: _, key: "", value: "message2"}}, 5_000
    Supervisor.stop(pid, :normal)
  end

  test "Elsa.Consumer will hand messages to the handler without state" do
    Agent.start_link(fn -> [] end, name: :test_message_store)

    {:ok, pid} =
      Elsa.Group.Supervisor.start_link(
        name: :name1,
        brokers: @brokers,
        topics: ["elsa-topic"],
        group: "group1",
        handler: Testing.ExampleMessageHandlerWithoutState,
        config: [begin_offset: :earliest]
      )

    send_messages(["message2"])

    assert_async 20, 500, fn ->
      messages = Agent.get(:test_message_store, fn s -> s end)
      assert 1 == length(messages)
      assert match?(%{topic: "elsa-topic", partition: 0, key: "", value: "message2"}, List.first(messages))
    end

    Supervisor.stop(pid)
  end

  defp send_messages(messages) do
    :brod.start_link_client([{'localhost', 9092}], :test_client)
    :brod.start_producer(:test_client, "elsa-topic", [])

    messages
    |> Enum.with_index()
    |> Enum.each(fn {msg, index} ->
      partition = rem(index, 2)
      :brod.produce_sync(:test_client, "elsa-topic", partition, "", msg)
    end)
  end
end

defmodule Testing.ExampleMessageHandlerWithState do
  use Elsa.Consumer.MessageHandler

  def init(args) do
    IO.inspect(args, label: "handler init")
    {:ok, args}
  end

  def handle_messages(messages, state) do
    IO.inspect(messages, label: "handler messages")
    Enum.each(messages, &send(state.pid, {:message, &1}))

    {:ack, state}
  end
end

defmodule Testing.ExampleMessageHandlerWithoutState do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages) do
    msgs = Enum.map(messages, &Map.delete(&1, :offset))
    Agent.update(:test_message_store, fn s -> s ++ msgs end)
    :ack
  end
end
