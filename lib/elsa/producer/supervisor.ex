defmodule Elsa.Producer.Supervisor do
  @moduledoc """
  Define a supervisor to create and manage producer processes, one
  per topic partition.
  """
  use Supervisor, restart: :transient

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
      %{
        id: name,
        start: {Elsa.Util, :start_client, [endpoints, name]}
      }

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
