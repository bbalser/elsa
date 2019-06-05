defmodule Elsa.ProducerTest do
  use ExUnit.Case
  use Divo
  alias Elsa.Producer
  require Elsa

  @brokers [{'localhost', 9092}]

  test "produces to preconfigured broker, topic, partition" do
    Elsa.create_topic(@brokers, "produce-topic-1")

    {:ok, _} =
      Producer.start_link(
        name: :producer1,
        brokers: @brokers,
        topic: "produce-topic-1",
        partition: 0
      )

    Producer.produce_sync(:producer1, "key1", "value1")
    Producer.produce_sync(:producer1, "key2", "value2")

    {:ok, {_count, messages}} = :brod.fetch(@brokers, "produce-topic-1", 0, 0)
    parsed_messages =
      Enum.map(messages, fn msg -> {Elsa.kafka_message(msg, :key), Elsa.kafka_message(msg, :value)} end)

    assert [{"key1", "value1"}, {"key2", "value2"}] == parsed_messages
  end
end
