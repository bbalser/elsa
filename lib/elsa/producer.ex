defmodule Elsa.Producer do
  def produce_sync(endpoints, topic, partition, key, value) do
    endpoints
    |> Elsa.Util.reformat_endpoints()
    |> :brod.start_client(Elsa.default_client())

    :brod.start_producer(Elsa.default_client(), topic, [])
    :brod.produce_sync(Elsa.default_client(), topic, partition, key, value)
  end
end
