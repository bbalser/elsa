defmodule Elsa.Topic do
  import Elsa.Util, only: [with_connection: 2, reformat_endpoints: 1]
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kpro_rsp, extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl")

  def list(endpoints) do
    {:ok, metadata} = :brod.get_metadata(reformat_endpoints(endpoints), :all)

    metadata.topic_metadata
    |> Enum.map(fn topic_metadata ->
      {topic_metadata.topic, Enum.count(topic_metadata.partition_metadata)}
    end)
  end

  def create(endpoints, topic, opts \\ []) do
    with_connection(endpoints, fn connection ->
      create_topic_args = %{
        topic: topic,
        num_partitions: Keyword.get(opts, :partitions, 1),
        replication_factor: Keyword.get(opts, :replicas, 1),
        replica_assignment: [],
        config_entries: []
      }

      version = Elsa.Util.get_api_version(connection, :create_topics)
      topic_request = :kpro_req_lib.create_topics(version, [create_topic_args], %{timeout: 5_000})

      send_request(connection, topic_request, 5_000)
    end)
  end

  defp send_request(connection, request, timeout) do
    case :kpro.request_sync(connection, request, timeout) do
      {:ok, response} -> check_response(response)
      result -> result
    end
  end

  defp check_response(response) do
    message = kpro_rsp(response, :msg)
    case Enum.find(message.topic_errors, fn error -> error.error_code != :no_error end) do
      nil -> :ok
      error -> {:error, error.error_message}
    end
  end
end
