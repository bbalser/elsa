defmodule Elsa.Group.DirectAcknowledger do
  @moduledoc """
  Handles acknowledgement of messages directly back to the Kafka
  cluster rather than the default behavior of routing acks through
  the interposing group coordinator supplied by brod. This allows for
  a drain functionality when the group coordinator has been told to
  exit and thus will no longer route pending acks to the cluster.
  """
  use GenServer
  require Logger

  import Elsa.Supervisor, only: [registry: 1]

  @timeout 5_000

  @doc """
  Route the offset and generation id for a given topic and partition to
  the internal callback for acknowledgement.
  """
  @spec ack(pid(), term(), Elsa.topic(), Elsa.partition(), Elsa.Group.Manager.generation_id(), integer()) :: :ok
  def ack(server, member_id, topic, partition, generation_id, offset) do
    GenServer.call(server, {:ack, member_id, topic, partition, generation_id, offset})
  end

  @doc """
  Start the direct acknowledger GenServer and link it to the calling process.
  Establishes a connection to the kafka cluster based on connection configuration
  of the brod group coordinator and saves it in the process state for later use
  when sending acknowledgements.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def init(opts) do
    state = %{
      connection: Keyword.fetch!(opts, :connection),
      group: Keyword.fetch!(opts, :group)
    }

    {:ok, state, {:continue, :connect}}
  end

  def handle_continue(:connect, state) do
    with brod_client <- Elsa.Registry.whereis_name({registry(state.connection), :brod_client}),
         {:ok, {endpoint, conn_config}} <- :brod_client.get_group_coordinator(brod_client, state.group),
         {:ok, kafka_connection} <- :kpro.connect(endpoint, conn_config) do
      Logger.debug(fn ->
        "#{__MODULE__}: Coordinator available for group #{state.group} on connection #{state.connection}"
      end)

      {:noreply, Map.put(state, :kafka_connection, kafka_connection)}
    else
      {:error, reason} when is_list(reason) ->
        case Keyword.get(reason, :error_code) do
          :coordinator_not_available ->
            Process.sleep(1_000)
            {:noreply, state, {:continue, :connect}}

          _ ->
            {:stop, reason, state}
        end

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_call({:ack, member_id, topic, partition, generation_id, offset}, _from, state) do
    request = make_request_body(state, member_id, topic, partition, generation_id, offset)

    with {:ok, response} <- :brod_utils.request_sync(state.kafka_connection, request, @timeout),
         :ok <- parse_response(response) do
      :ok = Elsa.Group.Consumer.ack(state.connection, topic, partition, offset)
      {:reply, :ok, state}
    else
      {:error, reason} ->
        Logger.error(
          "#{__MODULE__} : Received error when attempting to ack offset for #{topic}:#{partition}, reason: #{
            inspect(reason)
          }"
        )

        {:stop, reason, state}
    end
  end

  defp make_request_body(state, member_id, topic, partition, generation_id, offset) do
    partitions = [
      %{
        partition: partition,
        offset: offset + 1,
        metadata: "+1/#{:io_lib.format("~p/~p", [node(), self()])}"
      }
    ]

    topics = [
      %{
        topic: topic,
        partitions: partitions
      }
    ]

    request_body = %{
      group_id: state.group,
      generation_id: generation_id,
      member_id: member_id,
      retention_time: -1,
      topics: topics
    }

    :brod_kafka_request.offset_commit(state.kafka_connection, request_body)
  end

  defp parse_response(response) do
    case parse_offset_commit_response(response) do
      [] -> :ok
      errors -> {:error, errors}
    end
  end

  defp parse_offset_commit_response(response) do
    response.responses
    |> Enum.map(&parse_partition_responses/1)
    |> List.flatten()
  end

  defp parse_partition_responses(%{topic: topic, partition_responses: responses}) do
    responses
    |> Enum.filter(fn %{error_code: code} -> code != :no_error end)
    |> Enum.map(fn %{error_code: code, partition: partition} -> %{topic: topic, error: code, partition: partition} end)
  end
end
