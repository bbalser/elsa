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

  defdelegate produce_sync(endpoints, topic, partition, key, value), to: Elsa.Producer, as: :produce_sync

  def fetch() do
  end

  def default_client(), do: :elsa_default_client

  defmodule ConnectError do
    defexception [:message]
  end
end
