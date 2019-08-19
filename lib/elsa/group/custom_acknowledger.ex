defmodule Elsa.Group.CustomAcknowledger do
  use GenServer

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
      {:noreply, Map.put(state, :connection, connection)}
    else
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_call({:ack, member_id, topic, partition, generation_id, offset}, _from, state) do
    request = make_request_body(state, member_id, topic, partition, generation_id, offset)
    response = :brod_utils.request_sync(state.connection, request, @timeout)
    IO.inspect(response, label: "Response")

    {:reply, :ok, state}
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
end
