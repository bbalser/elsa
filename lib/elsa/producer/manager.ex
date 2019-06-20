defmodule Elsa.Producer.Manager do
  @moduledoc """
  Defines functions for starting persistent kafka
  producer processes that can be managed by a consuming
  application supervisor.
  """

  @doc """
  Start a named process for handling subsequent produce_sync requests to write
  messages to a topic. Producer processes are bound to a specific topic. The
  producer process requires and is managed by a client process, so the client pid
  is returned to allow for adding the client (and thus the producer) to an
  application's supervision tree.
  If the name option is not supplied to the producer's config, the default client
  name is used.
  """
  @spec start_producer(keyword(), String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_producer(endpoints, topic, config \\ []) when is_list(endpoints) do
    name = Keyword.get(config, :name, Elsa.default_client())

    case Elsa.Util.start_client(endpoints, name) do
      {:ok, client_pid} ->
        :brod.start_producer(name, topic, config)
        {:ok, client_pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stops the named producer process and cleans up entries from the supervisor's child
  process table in ets.
  """
  @spec stop_producer(atom(), String.t()) :: :ok | no_return()
  def stop_producer(client \\ Elsa.default_client(), topic), do: :brod_client.stop_producer(client, topic)
end
