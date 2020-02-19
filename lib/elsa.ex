defmodule Elsa do
  @moduledoc """
  Provides public api to Elsa. Top-level short-cuts to sub-module functions
  for performing basic interactions with Kafka including listing, creating,
  deleting, and validating topics. Also provides a function for one-off
  produce_sync of message(s) to a topic.
  """

  @typedoc "named connection, must be an atom"
  @type connection :: atom
  @type hostname :: atom | String.t()
  @type portnum :: pos_integer
  @typedoc "endpoints to connect to kafka brokers"
  @type endpoints :: [{hostname, portnum}]
  @type topic :: String.t()
  @type partition :: non_neg_integer

  defdelegate list_topics(endpoints), to: Elsa.Topic, as: :list

  defdelegate topic?(endpoints, topic), to: Elsa.Topic, as: :exists?

  defdelegate create_topic(endpoints, topic, opts \\ []), to: Elsa.Topic, as: :create

  defdelegate delete_topic(endpoints, topic), to: Elsa.Topic, as: :delete

  defdelegate produce(endpoints_or_connection, topic, messages, opts \\ []), to: Elsa.Producer

  defdelegate fetch(endpoints, topic, opts \\ []), to: Elsa.Fetch

  @doc """
  Define a default client name for establishing persistent connections to
  the Kafka cluster by producers and consumers. Useful for optimizing
  interactions by passing the identifier of a standing connection instead
  of instantiating a new one at each interaction, but when only a single connection
  is required, aleviating the need for the caller to differentiate and pass
  around a name.
  """
  @spec default_client() :: atom()
  def default_client(), do: :elsa_default_client

  defmodule Message do
    @moduledoc """
    Defines the structure of a Kafka message provided by the Elsa library and
    the function to construct the message struct.
    """
    import Record, only: [defrecord: 2, extract: 2]

    defrecord :kafka_message, extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")

    @type kafka_message :: record(:kafka_message, key: term(), value: term(), offset: integer(), ts: integer())
    @type elsa_message :: %Elsa.Message{
            topic: Elsa.topic(),
            partition: Elsa.partition(),
            offset: integer,
            key: term,
            value: term,
            generation_id: integer | nil,
            headers: list
          }

    defstruct [
      :topic,
      :partition,
      :offset,
      :key,
      :value,
      :timestamp,
      :generation_id,
      :headers
    ]

    @doc """
    Constructs a message struct from the imported definition of a kafka_message as
    defined by the brod library with the addition of the topic and partition the message
    was read from as well as the optional generation id as defined by the message's relationship
    to a consumer group. Generation id defaults to `nil` in the event the message retrieved
    outside of the context of a consumer group.
    """

    @spec new(kafka_message(), keyword()) :: elsa_message()
    def new(kafka_message(offset: offset, key: key, value: value, ts: timestamp, headers: headers), attributes) do
      %Message{
        topic: Keyword.fetch!(attributes, :topic),
        partition: Keyword.fetch!(attributes, :partition),
        offset: offset,
        key: key,
        value: value,
        timestamp: timestamp,
        generation_id: Keyword.get(attributes, :generation_id),
        headers: headers
      }
    end
  end

  defmodule ConnectError do
    defexception [:message]
  end
end
