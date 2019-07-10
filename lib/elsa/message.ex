defmodule Message do
  @moduledoc """
  Defines the structure of a Kafka message provided by the Elsa library and
  the function to construct the message struct.
  """
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message, extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")

  @type kafka_message :: record(:kafka_message, key: term(), value: term(), offset: integer(), ts: integer())
  @type elsa_message :: %Elsa.Message{
          topic: String.t(),
          partition: integer(),
          offset: integer(),
          key: term(),
          value: term(),
          generation_id: integer() | nil
        }

  defstruct [
    :topic,
    :partition,
    :offset,
    :key,
    :value,
    :timestamp,
    :generation_id
  ]

  @doc """
  Constructs a message struct from the imported definition of a kafka_message as
  defined by the brod library with the addition of the topic and partition the message
  was read from as well as the optional generation id as defined by the message's relationship
  to a consumer group. Generation id defaults to `nil` in the event the message retrieved
  outside of the context of a consumer group.
  """

  @spec new(kafka_message(), keyword()) :: elsa_message()
  def new(kafka_message(offset: offset, key: key, value: value, ts: timestamp), attributes) do
    %Message{
      topic: Keyword.fetch!(attributes, :topic),
      partition: Keyword.fetch!(attributes, :partition),
      offset: offset,
      key: key,
      value: value,
      timestamp: timestamp,
      generation_id: Keyword.get(attributes, :generation_id)
    }
  end
end
