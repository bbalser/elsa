defmodule Elsa.Producer.Supervisor do
  @moduledoc """
  Define a supervisor to create and manage producer processes for
  a given topic, one per topic partition. The supervisor will start
  a client if one is not already started and passed by name as an
  argument to the supervisor.
  """
  use Supervisor, restart: :transient

  @doc """
  Start a named process for handling subsequent produce_sync requests to write
  messages to a topic. Producer processes are bound to a specific topic.
  """
  @spec start_link(name: atom(), endpoints: keyword(), topic: String.t()) :: {:ok, pid()}
  def start_link(init_opts) do
    name = Keyword.fetch!(init_opts, :name)
    supervisor_name = supervisor_name(name)

    Supervisor.start_link(__MODULE__, init_opts, name: supervisor_name)
  end

  @impl Supervisor
  def init(init_opts) do
    name = Keyword.fetch!(init_opts, :name)
    endpoints = Keyword.fetch!(init_opts, :endpoints)
    topic = Keyword.fetch!(init_opts, :topic)

    :ok = Elsa.Util.start_client(endpoints, name)
    num_partitions = :brod.get_partitions_count(name, topic)

    partition_producers =
      Enum.map(0..(num_partitions - 1), fn partition ->
        %{
          id: :"elsa_producer_#{topic}_#{partition}",
          start: {:brod_producer, :start_link, [name, topic, partition, []]}
        }
      end)

    Supervisor.init(partition_producers, strategy: :one_for_one)
  end

  defp supervisor_name(name), do: :"elsa_producer_supervisor_#{name}"
end
