defmodule Elsa.Consumer do
  def start_link(opts) do
    client = Keyword.fetch!(opts, :client)
    brokers = Keyword.fetch!(opts, :brokers)
    topics = Keyword.fetch!(opts, :topics)
    consumer_group = Keyword.fetch!(opts, :consumer_group)
    handler = Keyword.fetch!(opts, :handler)
    handler_init_args = Keyword.get(opts, :handler_init_args, nil)

    endpoints = Enum.map(brokers, fn {host, port} -> {to_charlist(host), port} end)

    group_config = [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5]
    consumer_config = [begin_offset: :earliest]

    result = :brod.start_link_client(endpoints, client)

    {:ok, subscriber} =
      :brod.start_link_group_subscriber(
        client,
        consumer_group,
        topics,
        group_config,
        consumer_config,
        Elsa.Consumer.MessageHandler,
        %{handler: handler, init_args: handler_init_args, handler_state: nil}
      )

    result
  end
end
