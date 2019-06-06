defmodule Elsa.Producer do
  @moduledoc """
  Defines functions to manage producer supervisors and works,
  as well as send messages to topics.
  """

  def produce_sync_simple(endpoints, topic, partition, key, value) do
    client = Elsa.default_client()

    start_producer(endpoints, topic, name: client)
    produce_sync(client, topic, partition, key, value)
  end

  def start_producer(endpoints, topic, config \\ []) do
    name = Keyword.get(config, :name, Elsa.default_client())

    start_client(endpoints, name)
    :brod.start_producer(name, topic, config)
  end

  def produce_sync(client \\ Elsa.default_client(), topic, partition \\ 0, key, value) do
    :brod.produce_sync(client, topic, partition, key, value)
  end

  defp start_client(endpoints, name) do
    endpoints
    |> Elsa.Util.reformat_endpoints()
    |> :brod.start_client(name)
  end
end
