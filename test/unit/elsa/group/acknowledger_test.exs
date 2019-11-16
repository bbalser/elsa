defmodule Elsa.Group.AcknowledgerTest do
  use ExUnit.Case
  use Placebo

  alias Elsa.{Registry,Supervisor}
  alias Elsa.Group.Acknowledger

  setup do
    {:ok, registry} = Registry.start_link(name: Supervisor.registry(:connection))
    :yes = Registry.register_name({registry, :brod_group_coordinator}, self())
    {:ok, acknowledger} = Acknowledger.start_link(connection: :connection)
    Process.unlink(registry)
    Process.unlink(acknowledger)

    Acknowledger.update_generation_id(acknowledger, 1)
    Acknowledger.ack(:connection, "elsa-topic", 0, 1, 0)
    Acknowledger.ack(:connection, "elsa-topic", 1, 1, 0)
    Acknowledger.ack(:connection, "elsa-topic", 0, 1, 1)

    on_exit(fn ->
      Process.exit(acknowledger, :kill)
      Process.exit(registry, :kill)
    end)

    [acknowledger: acknowledger, registry: registry]
  end

  test "returns the latest offsets", %{acknowledger: acknowledger} do
    assert 2 == Acknowledger.get_latest_offset(acknowledger, "elsa-topic", 0)
    assert 1 == Acknowledger.get_latest_offset(acknowledger, "elsa-topic", 1)
  end

  test "acks messages and increments the offset", %{acknowledger: acknowledger, registry: registry} do
    :yes = Registry.register_name({registry, :"consumer_elsa-topic_0"}, self())
    :ok = Acknowledger.ack(:connection, "elsa-topic", 0, 1, 2)

    Process.sleep(50)

    assert_received {:"$gen_cast", {:ack, 2}}
    assert_received {:ack, 1, "elsa-topic", 0, 2}
    assert 3 == Acknowledger.get_latest_offset(acknowledger, "elsa-topic", 0)
  end

  test "updates generation id", %{acknowledger: acknowledger} do
    :ok = Acknowledger.update_generation_id(acknowledger, 2)

    assert 2 == :sys.get_state(acknowledger).generation_id
  end
end
