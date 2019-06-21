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

  defmodule ConnectError do
    defexception [:message]
  end
end
