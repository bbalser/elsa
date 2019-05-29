defmodule Elsa.Consumer.GroupMember do
  @behaviour :brod_group_member
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl :brod_group_member
  def assignments_received(pid, group_member_id, generation_id, assignments) do
    GenServer.cast(pid, {:assignments_received, generation_id, assignments})
  end

  @impl :brod_group_member
  def assignments_revoked(pid) do
    IO.inspect(pid, label: "pid")
    :ok
  end

  @impl :brod_group_member
  def user_data(pid) do
    "Elsa"
  end

  @impl GenServer
  def init(opts) do
    client = Keyword.fetch!(opts, :client)
    consumer_group = Keyword.fetch!(opts, :consumer_group)
    topics = Keyword.fetch!(opts, :topics)
    config = Keyword.get(opts, :config, [])

    Enum.each(topics, fn topic -> :brod.start_consumer(client, topic, []) end)

    {:ok, group_coordinator_pid} =
      :brod_group_coordinator.start_link(client, consumer_group, topics, config, __MODULE__, self())

    {:ok,
     %{
       client: client,
       consumer_group: consumer_group,
       topics: topics,
       config: config,
       group_coordinator_pid: group_coordinator_pid
     }}
  end

  def handle_cast({:assignments_received, generation_id, assignments}, state) do
    Enum.each(assignments, fn {:brod_received_assignment, topic, partition, offset} ->
      Elsa.Consumer.Worker.start_link(
        client: state.client,
        topic: topic,
        partition: partition,
        config: [begin_offset: determine_offset(offset)]
      )
    end)

    {:noreply, state}
  end

  defp determine_offset(:undefined), do: :earliest
  defp determine_offset(offset), do: offset
end
