defmodule Elsa.Group.SubscriberDeadTest do
  use ExUnit.Case
  use Divo

  @brokers Application.get_env(:elsa, :brokers)

  test "dead subscriber" do
    {:ok, pid} =
      Elsa.Group.Supervisor.start_link(
        name: :name1,
        brokers: @brokers,
        group: "group1",
        topics: ["elsa-topic"],
        handler: Test.BasicHandler,
        handler_init_args: %{pid: self()},
        config: [begin_offset: :earliest]
      )

    send_messages(0, ["message1"])
    send_messages(1, ["message2"])

    assert_receive {:message, %{value: "message1"}}, 5_000
    assert_receive {:message, %{value: "message2"}}, 5_000
    refute_receive {:message, _message}, 5_000

    kill_worker(0)

    send_messages(0, ["message3"])
    send_messages(1, ["message4"])

    assert_receive {:message, %{value: "message3"}}, 5_000
    assert_receive {:message, %{value: "message4"}}, 5_000
    refute_receive {:message, _message}, 5_000

    kill_worker(1)

    send_messages(0, ["message5"])
    send_messages(1, ["message6"])

    assert_receive {:message, %{value: "message5"}}, 5_000
    assert_receive {:message, %{value: "message6"}}, 5_000
    refute_receive {:message, _message}, 5_000

    Supervisor.stop(pid)
  end

  defp send_messages(partition, messages) do
    :brod.start_link_client([{'localhost', 9092}], :test_client)
    :brod.start_producer(:test_client, "elsa-topic", [])

    messages
    |> Enum.each(fn msg ->
      :brod.produce_sync(:test_client, "elsa-topic", partition, "", msg)
    end)
  end

  defp kill_worker(partition) do
    [{worker_pid, _value}] = Registry.lookup(:elsa_registry_name1, :"worker_elsa-topic_#{partition}")
    Process.exit(worker_pid, :kill)
    assert false == Process.alive?(worker_pid)
  end
end

defmodule Test.BasicHandler do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages, state) do
    Enum.each(messages, &send(state.pid, {:message, &1}))
    {:ack, state}
  end
end
