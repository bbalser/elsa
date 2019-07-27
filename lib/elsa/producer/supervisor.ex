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

    num_partitions = Elsa.Util.partition_count(endpoints, topic)

    client =
      case Process.whereis(name) do
        nil ->
          %{
            id: name,
            start: {Elsa.Util, :start_client, [endpoints, name]}
          }

        _pid ->
          []
      end

    partition_producers =
      Enum.map(0..(num_partitions - 1), fn partition ->
        %{
          id: :"elsa_producer_#{topic}_#{partition}",
          start: {:brod_producer, :start_link, [name, topic, partition, []]}
        }
      end)

    children =
      [
        client,
        partition_producers
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp supervisor_name(name), do: :"elsa_producer_supervisor_#{name}"
end
