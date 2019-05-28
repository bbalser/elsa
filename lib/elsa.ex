defmodule Elsa do
  @moduledoc """
  Documentation for Elsa.
  """

  import Record

  defrecord :kafka_message, extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")

  defdelegate list_topics(endpoints), to: Elsa.Topic, as: :list

  defdelegate create_topic(endpoints, topic, opts \\ []), to: Elsa.Topic, as: :create

  defdelegate produce_sync(endpoints, topic, partition, key, value), to: Elsa.Producer

  def delete_topic() do
  end

  def fetch() do
  end

  def default_client() do
    :elsa_default_client
  end

  defmodule ConnectError do
    defexception [:message]
  end
end
