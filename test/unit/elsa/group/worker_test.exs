defmodule Elsa.Group.WorkerTest do
  use ExUnit.Case
  use Placebo
  import Checkov
  import Elsa.Group.Worker, only: [kafka_message_set: 1]
  import Elsa.Message, only: [kafka_message: 1]

  describe "handle_info/2" do
    setup do
      Registry.start_link(keys: :unique, name: Elsa.Group.Supervisor.registry(:test_name))
      allow Elsa.Group.Manager.ack(any(), any(), any(), any(), any()), return: :ok
      allow :brod.subscribe(any(), any(), any(), any(), any()), return: {:ok, self()}, meck_options: [:passthrough]
      allow :brod.consume_ack(any(), any(), any(), any()), return: :ok, meck_options: [:passthrough]

      on_exit(fn ->
        pid = Process.whereis(__MODULE__)

        if pid != nil do
          ref = Process.monitor(pid)
          Process.exit(pid, :normal)
          assert_receive {:DOWN, ^ref, _, _, _}
        end
      end)

      init_args = [
        name: :test_name,
        topic: "test-topic",
        partition: 0,
        generation_id: 5,
        begin_offset: 13,
        handler: Elsa.Group.WorkerTest.Handler,
        handler_init_args: [],
        config: []
      ]

      Elsa.Group.Worker.start_link(init_args)

      messages =
        kafka_message_set(
          topic: "test-topic",
          partition: 0,
          messages: [
            kafka_message(offset: 13, key: "key1", value: "value1"),
            kafka_message(offset: 14, key: "key2", value: "value2")
          ]
        )

      [messages: messages, state: create_state(init_args)]
    end

    data_test "handler can specifiy offset to ack", %{messages: messages, state: state} do
      set_handler(fn messages ->
        offset = messages |> List.first() |> Map.get(:offset)
        {ack, offset}
      end)

      Elsa.Group.Worker.handle_info({:some_pid, messages}, state)

      assert_called Elsa.Group.Manager.ack(:test_name, "test-topic", 0, 5, 13)

      where ack: [:ack, :acknowledge]
    end

    data_test "handler can say #{response}", %{messages: messages, state: state} do
      set_handler(fn _messags -> response end)

      Elsa.Group.Worker.handle_info({:some_pid, messages}, state)

      refute_called Elsa.Group.Manager.ack(:test_name, "test-topic", 0, any(), any())
      refute_called :brod.consume_ack(:test_name, "test-topic", 0, any())
      where response: [:no_ack, :noop]
    end

    test "handler can say to continue to consume the ack but not ack consumer group", %{
      messages: messages,
      state: state
    } do
      set_handler(fn _messages -> :continue end)

      Elsa.Group.Worker.handle_info({:some_pid, messages}, state)

      refute_called Elsa.Group.Manager.ack(:test_name, "test-topic", 0, any(), any())
      assert_called :brod.consume_ack(:test_name, "test-topic", 0, any())
    end
  end

  defp create_state(init_args) do
    state =
      init_args
      |> Enum.into(%{})
      |> Map.delete(:begin_offset)
      |> Map.put(:offset, 13)

    struct(Elsa.Group.Worker.State, state)
  end

  defp set_handler(handler) do
    Agent.start_link(fn -> handler end, name: __MODULE__)
  end
end

defmodule Elsa.Group.WorkerTest.Handler do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages) do
    function = Agent.get(Elsa.Group.WorkerTest, fn s -> s end)
    function.(messages)
  end
end
