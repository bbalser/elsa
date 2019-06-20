defmodule Elsa do
  @moduledoc """
  Provides public api to Elsa. Top-level short-cuts to sub-module functions
  for performing basic interactions with Kafka including listing, creating,
  deleting, and validating topics. Also provides a function for one-off
  produce_sync of message(s) to a topic.
  """

  import Record

  defrecord :kafka_message, extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")

  defdelegate list_topics(endpoints), to: Elsa.Topic, as: :list

  defdelegate topic?(endpoints, topic), to: Elsa.Topic, as: :exists?

  defdelegate create_topic(endpoints, topic, opts \\ []), to: Elsa.Topic, as: :create

  defdelegate delete_topic(endpoints, topic), to: Elsa.Topic, as: :delete

  defdelegate produce(endpoints, topic, messages, opts \\ []), to: Elsa.Producer

  defdelegate produce_sync(topic, messages, opts \\ []), to: Elsa.Producer

  @doc """
  A simple interface for quickly retrieving a message set from the cluster
  at the given topic. Partition and offset may be specified as keyword options,
  defaulting to 0 in both cases if either is not supplied by the caller.
  """
  @spec fetch(keyword(), String.t(), keyword()) :: {:ok, {integer(), [tuple()]}} | {:error, term()}
  def fetch(endpoints, topic, opts \\ []) do
    partition = Keyword.get(opts, :partition, 0)
    offset = Keyword.get(opts, :offset, 0)

    case :brod.fetch(endpoints, topic, partition, offset) do
      {:ok, {partition_offset, messages}} ->
        stripped_messages = Enum.map(messages, &strip_messages/1)
        {:ok, partition_offset, stripped_messages}
      {:error, reason} ->
        {:error, reason}
    end
  end

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

  defmodule ConnectError do
    defexception [:message]
  end

  defp strip_messages({_, offset, key, value, _, _, _}), do: {offset, key, value}
end
