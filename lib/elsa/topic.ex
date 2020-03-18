defmodule Elsa.Topic do
  @moduledoc """
  Provides functions for managing and interacting with topics in the Kafka cluster.
  """
  import Elsa.Util, only: [with_connection: 3, reformat_endpoints: 1]
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kpro_rsp, extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl")

  @doc """
  Returns a list of all topics managed by the cluster as tuple of topic name and
  number of partitions.
  """
  @spec list(keyword) :: {:ok, [{String.t(), integer}]} | {:error, term}
  def list(endpoints) do
    {:ok, metadata} = :brod.get_metadata(reformat_endpoints(endpoints), :all)

    topics =
      metadata.topic_metadata
      |> Enum.map(fn topic_metadata ->
        {topic_metadata.topic, Enum.count(topic_metadata.partition_metadata)}
      end)

    {:ok, topics}
  catch
    error -> {:error, error}
  end

  @doc """
  Confirms or denies the existence of a topic managed by the cluster.
  """
  @spec exists?(keyword(), String.t()) :: boolean()
  def exists?(endpoints, topic) do
    with {:ok, topics} <- list(endpoints) do
      Enum.any?(topics, fn {t, _} -> t == topic end)
    end
  end

  @doc """
  Creates the supplied topic within the cluster. Sets the number of desired
  partitions and replication factor for the topic based on the optional
  keyword list. If the optional configs are not specified by the caller, the
  number of partitions and replicas defaults to 1.
  """
  @spec create(keyword(), String.t(), keyword()) :: :ok | {:error, term()}
  def create(endpoints, topic, opts \\ []) do
    with_connection(endpoints, :controller, fn connection ->
      config =
        opts
        |> Keyword.get(:config, [])
        |> Enum.map(fn {key, val} -> %{config_name: to_string(key), config_value: val} end)

      create_topic_args = %{
        topic: topic,
        num_partitions: Keyword.get(opts, :partitions, 1),
        replication_factor: Keyword.get(opts, :replicas, 1),
        replica_assignment: [],
        config_entries: config
      }

      version = Elsa.Util.get_api_version(connection, :create_topics)
      topic_request = :kpro_req_lib.create_topics(version, [create_topic_args], %{timeout: 5_000})

      send_request(connection, topic_request, 5_000)
    end)
  end

  @doc """
  Deletes the supplied topic from the cluster.
  """
  @spec delete(keyword(), String.t()) :: :ok | {:error, term()}
  def delete(endpoints, topic) do
    with_connection(endpoints, :controller, fn connection ->
      version = Elsa.Util.get_api_version(connection, :delete_topics)
      topic_request = :kpro_req_lib.delete_topics(version, [topic], %{timeout: 5_000})

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

    error_key =
      case Map.has_key?(message, :topic_errors) do
        true -> :topic_errors
        false -> :topic_error_codes
      end

    case Enum.find(message[error_key], fn error -> error.error_code != :no_error end) do
      nil -> :ok
      error -> {:error, {error.error_code, error[:error_message]}}
    end
  end
end
