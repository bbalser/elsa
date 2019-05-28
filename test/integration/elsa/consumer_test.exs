defmodule Elsa.ConsumerTest do
  use ExUnit.Case
  use Divo
  require Logger

  test "Elsa.Consumer will hand messages to the handler with state" do
    brokers = [localhost: 9092]

    Elsa.Consumer.start_link(
      client: :client1,
      brokers: brokers,
      topics: ["elsa-topic"],
      consumer_group: "group1",
      handler: Testing.ExampleMessageHandlerWithState,
      handler_init_args: %{pid: self()}
    )

    send_messages(["message1", "message2"])

    assert_receive {:message, %{topic: "elsa-topic", partition: 0, offset: _, key: "", value: "message1"}}, 5_000
    assert_receive {:message, %{topic: "elsa-topic", partition: 1, offset: _, key: "", value: "message2"}}, 5_000
  end

  test "Elsa.Consumer will hand messages to the handler without state" do
    brokers = [localhost: 9092]

    Elsa.Consumer.start_link(
      client: :client1,
      brokers: brokers,
      topics: ["elsa-topic"],
      consumer_group: "group1",
      handler: Testing.ExampleMessageHandlerWithoutState
    )

    send_messages(["message2"])

    Patiently.wait_for!(
      fn ->
        messages = Testing.ExampleMessageHandlerWithoutState.get_messages()
        Logger.info("Received messages: #{inspect(messages)}")

        match?(
          [%{topic: "elsa-topic", partition: 0, offset: _offset, key: "", value: "message2"}],
          messages
        )
      end,
      dwell: 500,
      max_tries: 20
    )
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
    {:ok, args}
  end

  def handle_message(message, state) do
    IO.inspect(message, label: "Message")
    IO.inspect(self(), label: "self")
    send(state.pid, {:message, message})

    {:ok, state}
  end
end

defmodule Testing.ExampleMessageHandlerWithoutState do
  use Elsa.Consumer.MessageHandler

  def init(_args) do
    Agent.start_link(fn -> [] end, name: __MODULE__)
    {:ok, []}
  end

  def get_messages() do
    Agent.get(__MODULE__, fn s -> Enum.reverse(s) end)
  end

  def handle_message(message) do
    Agent.update(__MODULE__, fn s -> [message | s] end)
    :ok
  end
end
