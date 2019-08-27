defmodule Elsa.Group.DirectAcknowledger do
  use GenServer
  require Logger

  @timeout 5_000

  def ack(server, member_id, topic, partition, generation_id, offset) do
    GenServer.call(server, {:ack, member_id, topic, partition, generation_id, offset})
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def init(opts) do
    state = %{
      client: Keyword.fetch!(opts, :client),
      group: Keyword.fetch!(opts, :group)
    }

    {:ok, state, {:continue, :connect}}
  end

  def handle_continue(:connect, state) do
    with {:ok, {endpoint, conn_config}} <- :brod_client.get_group_coordinator(state.client, state.group),
         {:ok, connection} <- :kpro.connect(endpoint, conn_config) do
      Logger.debug(fn -> "#{__MODULE__}: Coordinator available for group #{state.group} on client #{state.client}" end)
      {:noreply, Map.put(state, :connection, connection)}
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

    with {:ok, response} <- :brod_utils.request_sync(state.connection, request, @timeout),
         :ok <- parse_response(response) do
      :brod.consume_ack(state.client, topic, partition, offset)
      {:reply, :ok, state}
    else
      {:error, reason} -> {:stop, reason, state}
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

    :brod_kafka_request.offset_commit(state.connection, request_body)
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
