defmodule Elsa.Group.WorkerTest do
  use ExUnit.Case
  use Placebo
  import Elsa.Group.Worker, only: [kafka_message_set: 1, kafka_message: 1]

  describe "handle_info/2" do
    setup do
      Registry.start_link(keys: :unique, name: Elsa.Group.Supervisor.registry(:test_name))
      allow Elsa.Group.Manager.ack(any(), any(), any(), any(), any()), return: :ok
      allow :brod.subscribe(any(), any(), any(), any(), any()), return: {:ok, self()}
      allow :brod.consume_ack(any(), any(), any(), any()), return: :ok
      :ok
    end

    test "handler can specifiy offset to ack" do
      init_args = [
        name: :test_name,
        topic: "test-topic",
        partition: 0,
        generation_id: 5,
        begin_offset: 13,
        handler: Elsa.Group.WorkerTest.Handler1,
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

      Elsa.Group.Worker.handle_info({:some_pid, messages}, create_state(init_args))

      assert_called Elsa.Group.Manager.ack(:test_name, "test-topic", 0, 5, 13)
    end

    test "handler can say no_ack" do
      init_args = [
        name: :test_name,
        topic: "test-topic",
        partition: 0,
        generation_id: 5,
        begin_offset: 13,
        handler: Elsa.Group.WorkerTest.NoAck,
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

      Elsa.Group.Worker.handle_info({:some_pid, messages}, create_state(init_args))

      refute_called Elsa.Group.Manager.ack(:test_name, "test-topic", 0, any(), any())
      refute_called :brod.consume_ack(:test_name, "test-topic", 0, any())
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
end

defmodule Elsa.Group.WorkerTest.Handler1 do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages) do
    offset = messages |> List.first() |> Map.get(:offset)
    {:ack, offset}
  end
end

defmodule Elsa.Group.WorkerTest.NoAck do
  use Elsa.Consumer.MessageHandler

  def handle_messages(_messages) do
    :no_ack
  end
end
